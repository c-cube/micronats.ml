(** Minimal NATS client using Eio.

    The NATS protocol is a simple text-based line protocol. Each line is
    terminated by CRLF. Messages have a header line with the byte count,
    followed by the payload and a trailing CRLF. *)

let crlf = "\r\n"

(** {2 Connection state} *)

type sub_data = {
  queue: string option;
  f: ?reply_to:string -> string -> unit;
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

let send_connect t =
  write_str t ({|CONNECT {"verbose":false,"pedantic":false}|} ^ crlf)

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

(** {2 Message parsing} *)

let parse_msg_header line =
  try
    Scanf.sscanf line "MSG %s %d %d" (fun subject sid size ->
        subject, sid, None, size)
  with Scanf.Scan_failure _ | End_of_file ->
    Scanf.sscanf line "MSG %s %d %s %d" (fun subject sid reply_to size ->
        subject, sid, Some reply_to, size)

let dispatch_msg t ~sid ~reply_to payload =
  let f_opt =
    Eio.Mutex.use_rw ~protect:true t.subs_mutex (fun () ->
        Option.map (fun s -> s.f) (Hashtbl.find_opt t.subs sid))
  in
  Option.iter
    (fun f ->
      match f ?reply_to payload with
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
  | line when String.starts_with ~prefix:"MSG" line ->
    let _subject, sid, reply_to, size = parse_msg_header line in
    let payload = Eio.Buf_read.take size buf in
    ignore (Eio.Buf_read.take 2 buf);
    dispatch_msg t ~sid ~reply_to payload;
    reader_loop t buf
  | _ -> reader_loop t buf

(** {2 Connection lifecycle} *)

let connect_to ~sw ~net ~host:_ ~port () =
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
  send_connect t;
  Eio.Fiber.fork ~sw (fun () -> try reader_loop t buf with End_of_file -> ());
  t

let connect ~sw ~net () = connect_to ~sw ~net ~host:"localhost" ~port:4222 ()

(** {2 Public API} *)

let pub t ~subject ?reply_to payload = send_pub t ~subject ~reply_to ~payload

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
    sub t ~sw ~subject:inbox ~queue:None ~f:(fun ?reply_to:_ m ->
        Eio.Promise.resolve r m)
  in
  pub t ~subject ~reply_to:inbox payload;
  match
    Eio.Time.with_timeout clock timeout (fun () -> Ok (Eio.Promise.await p))
  with
  | Ok x -> Ok x
  | Error `Timeout -> Error `Timeout

let close t = t.shutdown ()
