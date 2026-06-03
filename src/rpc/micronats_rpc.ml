open Pbrt_services.Value_mode

open struct
  module Nats = Micronats
  module Client = Pbrt_services.Client
  module Server = Pbrt_services.Server
  module Log = (val Logs.src_log (Logs.Src.create "micronats.rpc"))
end

type handler =
  | H_one : ('req, unary, 'res, unary) Server.rpc * ('req -> 'res) -> handler
      (** One-to-one RPC, use a queue on server side *)
  | H_gather :
      ('req, stream, 'res, stream) Server.rpc * ('req -> 'res Eio.Stream.t)
      -> handler
      (** Each server replies with a (possibly empty) stream, all streams are
          merged on the client side *)

let mk_h_one rpc f : handler = H_one (rpc, f)
let mk_h_gather rpc f : handler = H_gather (rpc, f)

open struct
  let ( let@ ) = ( @@ )
  let spf = Printf.sprintf

  let try_catch prefix f =
    try f ()
    with exn ->
      Log.err (fun k ->
          k "Error while handling %s: %s" (String.concat "." prefix)
            (Printexc.to_string exn))

  let guess_enc (msg : Nats.msg) : [ `Proto | `Json ] =
    match Option.bind msg.headers (List.assoc_opt "encoding") with
    | Some "proto" -> `Proto
    | Some "json" | None -> `Json
    | Some other -> failwith (spf "unknown encoding %S" other)

  let decode_server (rpc : _ Server.rpc) (msg : Nats.msg) =
    match guess_enc msg with
    | `Proto ->
      let dec = Pbrt.Decoder.of_string msg.payload in
      rpc.decode_pb_req dec, `Proto
    | `Json ->
      let req = rpc.decode_json_req @@ Yojson.Basic.from_string msg.payload in
      req, `Json

  let encode_server (rpc : _ Server.rpc) encoding res : string =
    match encoding with
    | `Json -> rpc.encode_json_res res |> Yojson.Basic.to_string
    | `Proto ->
      let enc = Pbrt.Encoder.create () in
      rpc.encode_pb_res res enc;
      Pbrt.Encoder.to_string enc

  let decode_client (rpc : _ Client.rpc) encoding (msg : string) =
    match encoding with
    | `Proto ->
      let dec = Pbrt.Decoder.of_string msg in
      rpc.decode_pb_res dec
    | `Json -> rpc.decode_json_res @@ Yojson.Basic.from_string msg

  let encode_client (rpc : _ Client.rpc) encoding req : string =
    match encoding with
    | `Json -> rpc.encode_json_req req |> Yojson.Basic.to_string
    | `Proto ->
      let enc = Pbrt.Encoder.create () in
      rpc.encode_pb_req req enc;
      Pbrt.Encoder.to_string enc
end

(** Add service to [nats], with subscriptions to handle the requests *)
let add_service (nats : Nats.t) ~(sw : Eio.Switch.t) (server : handler Server.t)
    : unit =
  (* subject prefix for the whole service *)
  let prefix = [ "natsrpc" ] @ server.package @ [ server.service_name ] in

  let add_handler (h : handler) : unit =
    match h with
    | H_one (rpc, f) ->
      let queue = "micronatsrpc" in
      let subject = prefix @ [ rpc.name ] in
      Nats.sub nats ~sw ~subject ~queue (fun msg ->
          let@ () = try_catch subject in
          let reply_to =
            match msg.reply_to with
            | None -> failwith "expected a reply-to"
            | Some r -> r
          in
          let req, encoding = decode_server rpc msg in
          let res = f req in
          let res = encode_server rpc encoding res in
          Nats.pub nats ~subject:(String.split_on_char '.' reply_to) res)
      |> (ignore : Nats.sub -> unit)
    | H_gather (_rpc, _f) -> ()
    (* TODO: fix the inbox (we need it to survive multiple responses,
      and to be removed only upon timeout)

      let subject = prefix @ [ rpc.name ] in
      Nats.sub nats ~sw ~subject (fun msg ->
          let@ () = try_catch subject in
          let reply_to =
            match msg.reply_to with
            | None -> failwith "expected a reply-to"
            | Some r -> r
          in
          let req, encoding = decode rpc msg in
          let res_stream = f req in
        (* FIXME: the stream should contain options, so we can encode termination *)
        (* TODO: iterate on the stream, sending each result. *)
        )
      |> (ignore : Nats.sub -> unit)
      *)
  in

  List.iter add_handler server.handlers

let send_request (nats : Nats.t) ?(timeout = 10.) ~(sw : Eio.Switch.t) ~clock
    (rpc : ('req, unary, 'res, unary) Client.rpc) (req : 'req) : 'res =
  let subject =
    [ "natsrpc" ] @ rpc.package @ [ rpc.service_name; rpc.rpc_name ]
  in
  let@ () = try_catch subject in
  let encoding = `Json in
  match
    Nats.request nats ~sw ~clock ~subject ~timeout
      (encode_client rpc encoding req)
  with
  | Error `Timeout -> failwith "timeout"
  | Ok msg -> decode_client rpc encoding msg
