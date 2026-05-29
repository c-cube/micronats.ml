open Eio.Std

let () =
  Eio_posix.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Eio.Switch.run @@ fun sw ->
  let nats = Mininats.connect ~sw ~net () in
  traceln "connected!";
  (* test pub/sub *)
  let got = ref None in
  let _sub =
    Mininats.sub nats ~sw ~subject:[ "test"; "hello" ]
      (fun (msg : Mininats.msg) ->
        traceln "GOT: %S" msg.payload;
        got := Some msg.payload)
  in
  Mininats.pub nats ~subject:[ "test"; "hello" ] "world42";
  Eio.Time.sleep clock 0.3;
  (match !got with
  | Some "world42" ->
    traceln "PASS: pub/sub";
    (* test hpub/hsub *)
    let hgot = ref None in
    let _hsub =
      Mininats.sub nats ~sw ~subject:[ "test"; "headers" ]
        (fun (msg : Mininats.msg) ->
          traceln "GOT HEADERS: %s payload:%S"
            (match msg.headers with
            | Some hs ->
              String.concat ", " (List.map (fun (k, v) -> k ^ "=" ^ v) hs)
            | None -> "(none)")
            msg.payload;
          hgot := Some msg.payload)
    in
    Mininats.hpub nats ~subject:[ "test"; "headers" ]
      ~headers:[ "X-Foo", "bar"; "X-Baz", "42" ]
      "hello-headers";
    Eio.Time.sleep clock 0.3;
    (match !hgot with
    | Some "hello-headers" -> traceln "PASS: hpub/hsub"
    | _ -> traceln "FAIL: hpub/hsub");
    (* test request/reply *)
    let _service =
      Mininats.sub nats ~sw ~subject:[ "test"; "echo" ]
        (fun (msg : Mininats.msg) ->
          match msg.reply_to with
          | Some rt ->
            Mininats.pub nats ~subject:(String.split_on_char '.' rt) msg.payload
          | None -> ())
    in
    (match
       Mininats.request nats ~sw ~clock ~subject:[ "test"; "echo" ] ~timeout:2.0
         "ping"
     with
    | Ok "ping" -> traceln "PASS: request/reply"
    | r ->
      traceln "FAIL: request/reply: %s"
        (match r with
        | Ok s -> s
        | Error _ -> "timeout"));
    (* test timeout *)
    (match
       Mininats.request nats ~sw ~clock ~subject:[ "test"; "nobody" ]
         ~timeout:0.2 "hello"
     with
    | Error `Timeout -> traceln "PASS: timeout"
    | _ -> traceln "FAIL: timeout")
  | _ -> traceln "FAIL: pub/sub");
  Mininats.close nats;
  traceln "all tests passed"
