(** Minimal NATS client using Eio.

    The NATS protocol is a text-based line protocol. Lines end with CRLF.
    Messages have a header line with byte counts, followed by the payload and a
    trailing CRLF. Supports both [MSG] and [HMSG] (headers). *)

let crlf = "\r\n"
let hdr_line = "NATS/1.0\r\n"

(** {2 Connection state} *)

type header = string * string

type sub_data = {
  queue: string option;
  f: ?reply_to:string -> ?headers:header list -> string -> unit;
}

type t = {
  flow: Eio.Flow.sink_ty Eio.Flow.sink;
  write_mutex: Eio.Mutex.t;
  subs: (int, sub_data) Hashtbl.t;
  subs_mutex: Eio.Mutex.t;
  next_sid: int Atomic.t;
  shutdown: unit -> unit;
}

type sub = int

(** {2 Low-level I/O} *)

let write_str t s =
  Eio.Mutex.use_rw ~protect:true t.write_mutex (fun () ->
      try Eio.Flow.copy_string s t.flow with _ -> ())

(** {2 Protocol primitives} *)

let send_connect t ?token ?user ?pass () =
  let buf = Buffer.create 64 in
  Buffer.add_string buf
    {|CONNECT {"verbose":false,"pedantic":false,"headers":true|};
  Option.iter (fun t -> Printf.bprintf buf {|,"auth_token":"%s"|} t) token;
  Option.iter (fun u -> Printf.bprintf buf {|,"user":"%s"|} u) user;
  Option.iter (fun p -> Printf.bprintf buf {|,"pass":"%s"|} p) pass;
  Buffer.add_string buf "}";
  Buffer.add_string buf crlf;
  write_str t (Buffer.contents buf)

let send_pong t = write_str t ("PONG" ^ crlf)

let send_sub t ~sid subject queue =
  match queue with
  | None -> write_str t (Printf.sprintf "SUB %s %d%s" subject sid crlf)
  | Some q -> write_str t (Printf.sprintf "SUB %s %s %d%s" subject q sid crlf)

let send_unsub t ~sid ~max_msgs =
  match max_msgs with
  | None -> write_str t (Printf.sprintf "UNSUB %d%s" sid crlf)
  | Some n -> write_str t (Printf.sprintf "UNSUB %d %d%s" sid n crlf)

let send_pub t ~subject ~reply_to ~payload =
  let n = String.length payload in
  (match reply_to with
  | None -> write_str t (Printf.sprintf "PUB %s %d%s" subject n crlf)
  | Some rt -> write_str t (Printf.sprintf "PUB %s %s %d%s" subject rt n crlf));
  write_str t payload;
  write_str t crlf

let encode_headers hs =
  String.concat "" (List.map (fun (k, v) -> k ^ ": " ^ v ^ crlf) hs)

let send_hpub t ~subject ~reply_to ~headers ~payload =
  let hdr = hdr_line ^ encode_headers headers ^ crlf in
  let hdr_len = String.length hdr in
  let total_len = hdr_len + String.length payload in
  (match reply_to with
  | None ->
    write_str t
      (Printf.sprintf "HPUB %s %d %d%s" subject hdr_len total_len crlf)
  | Some rt ->
    write_str t
      (Printf.sprintf "HPUB %s %s %d %d%s" subject rt hdr_len total_len crlf));
  write_str t hdr;
  write_str t payload;
  write_str t crlf

(** {2 Message parsing} *)

type msg = {
  subject: string;
  sid: int;
  reply_to: string option;
  headers: header list option;
  payload: string;
}

let read_hmsg buf subject sid reply_to hdr_len total_len =
  let hdr_data = Eio.Buf_read.take hdr_len buf in
  let payload_len = total_len - hdr_len in
  let payload = Eio.Buf_read.take payload_len buf in
  ignore (Eio.Buf_read.take 2 buf);
  let headers =
    let hdr_line_len = String.length hdr_line in
    if hdr_len > hdr_line_len + 2 then (
      let raw = String.sub hdr_data hdr_line_len (hdr_len - hdr_line_len - 2) in
      let hs =
        String.split_on_char '\n' raw
        |> List.filter_map (fun line ->
               let line = String.trim line in
               if line = "" then
                 None
               else (
                 match String.index_opt line ':' with
                 | Some i ->
                   let k = String.trim (String.sub line 0 i) in
                   let v =
                     String.trim
                       (String.sub line (i + 1) (String.length line - i - 1))
                   in
                   Some (k, v)
                 | None -> None
               ))
      in
      if hs = [] then
        None
      else
        Some hs
    ) else
      None
  in
  { subject; sid; reply_to; headers; payload }

let parse_msg line buf =
  let parts = String.split_on_char ' ' line |> List.filter (fun s -> s <> "") in
  match parts with
  | [ "MSG"; subject; sid_s; size_s ] ->
    let payload = Eio.Buf_read.take (int_of_string size_s) buf in
    ignore (Eio.Buf_read.take 2 buf);
    {
      subject;
      sid = int_of_string sid_s;
      reply_to = None;
      headers = None;
      payload;
    }
  | [ "MSG"; subject; sid_s; reply_to; size_s ] ->
    let payload = Eio.Buf_read.take (int_of_string size_s) buf in
    ignore (Eio.Buf_read.take 2 buf);
    {
      subject;
      sid = int_of_string sid_s;
      reply_to = Some reply_to;
      headers = None;
      payload;
    }
  | [ "HMSG"; subject; sid_s; hdr_len_s; total_len_s ] ->
    let sid = int_of_string sid_s in
    let hdr_len = int_of_string hdr_len_s in
    let total_len = int_of_string total_len_s in
    read_hmsg buf subject sid None hdr_len total_len
  | [ "HMSG"; subject; sid_s; reply_to; hdr_len_s; total_len_s ] ->
    let sid = int_of_string sid_s in
    let hdr_len = int_of_string hdr_len_s in
    let total_len = int_of_string total_len_s in
    read_hmsg buf subject sid (Some reply_to) hdr_len total_len
  | _ -> failwith (Printf.sprintf "bad protocol line: %S" line)

let dispatch_msg t msg =
  let f_opt =
    Eio.Mutex.use_rw ~protect:true t.subs_mutex (fun () ->
        Option.map (fun s -> s.f) (Hashtbl.find_opt t.subs msg.sid))
  in
  Option.iter
    (fun f ->
      match f ?reply_to:msg.reply_to ?headers:msg.headers msg.payload with
      | () -> ()
      | exception _ -> ())
    f_opt

(** {2 Reader loop} *)

let rec reader_loop t buf =
  match Eio.Buf_read.line buf with
  | "PING" ->
    send_pong t;
    reader_loop t buf
  | "PONG" | "+OK" -> reader_loop t buf
  | line when String.starts_with ~prefix:"-ERR" line -> reader_loop t buf
  | line
    when String.starts_with ~prefix:"MSG" line
         || String.starts_with ~prefix:"HMSG" line ->
    let msg = parse_msg line buf in
    dispatch_msg t msg;
    reader_loop t buf
  | _ -> reader_loop t buf

(** {2 Connection lifecycle} *)

let connect_to ~sw ~net ?token ?user ?pass ~host:_ ~port () =
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, port) in
  let flow = Eio.Net.connect ~sw net addr in
  let buf = Eio.Buf_read.of_flow ~max_size:(1024 * 1024) flow in
  let t =
    {
      flow :> Eio.Flow.sink_ty Eio.Flow.sink;
      write_mutex = Eio.Mutex.create ();
      subs = Hashtbl.create 16;
      subs_mutex = Eio.Mutex.create ();
      next_sid = Atomic.make 1;
      shutdown = (fun () -> Eio.Flow.shutdown flow `All);
    }
  in
  let info_line = Eio.Buf_read.line buf in
  if not (String.starts_with ~prefix:"INFO" info_line) then
    failwith (Printf.sprintf "expected INFO, got: %S" info_line);
  send_connect t ?token ?user ?pass ();
  Eio.Fiber.fork ~sw (fun () -> try reader_loop t buf with End_of_file -> ());
  t

let connect ~sw ~net ?token ?user ?pass () =
  connect_to ~sw ~net ?token ?user ?pass ~host:"localhost" ~port:4222 ()

(** {2 Public API} *)

let pub t ~subject ?reply_to payload = send_pub t ~subject ~reply_to ~payload

let hpub t ~subject ?reply_to ?(headers = []) payload =
  send_hpub t ~subject ~reply_to ~headers ~payload

let unsub t ~max_msgs sid =
  send_unsub t ~sid ~max_msgs;
  Eio.Mutex.use_rw ~protect:true t.subs_mutex (fun () ->
      Hashtbl.remove t.subs sid)

let sub t ~sw ~subject ~queue ~f =
  let sid = Atomic.fetch_and_add t.next_sid 1 in
  send_sub t ~sid subject queue;
  Eio.Mutex.use_rw ~protect:true t.subs_mutex (fun () ->
      Hashtbl.replace t.subs sid { queue; f });
  Eio.Switch.on_release sw (fun () -> unsub t ~max_msgs:None sid);
  sid

let request t ~sw ~clock ~subject ~timeout payload =
  let inbox = Printf.sprintf "_INBOX.%06x" (Random.bits () land 0xFFFFFF) in
  let p, r = Eio.Promise.create () in
  let _sub =
    sub t ~sw ~subject:inbox ~queue:None ~f:(fun ?reply_to:_ ?headers:_ m ->
        Eio.Promise.resolve r m)
  in
  pub t ~subject ~reply_to:inbox payload;
  match
    Eio.Time.with_timeout clock timeout (fun () -> Ok (Eio.Promise.await p))
  with
  | Ok x -> Ok x
  | Error `Timeout -> Error `Timeout

let close t = t.shutdown ()

(** {2 Retry helper} *)

let with_retry ~clock ?(delay = 15.) ?(max_retries : int option) ~connect () f =
  let rec loop n =
    match connect () with
    | conn ->
      let result = f conn in
      close conn;
      result
    | exception exn ->
      (match max_retries with
      | Some max when n >= max -> raise exn
      | _ ->
        Eio.Time.sleep clock delay;
        loop (n + 1))
  in
  loop 1
