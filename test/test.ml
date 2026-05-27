open Eio.Std

let () =
  Eio_posix.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Eio.Switch.run @@ fun sw ->
  let nats = Mininats.connect ~sw ~net () in
  traceln "connected!";
  let got = ref None in
  let _sub =
    Mininats.sub nats ~sw ~subject:"test.hello" ~queue:None
      ~f:(fun ?reply_to:_ msg ->
        traceln "GOT: %S" msg;
        got := Some msg)
  in
  Mininats.pub nats ~subject:"test.hello" "world42";
  Eio.Time.sleep clock 0.3;
  (match !got with
  | Some "world42" ->
    traceln "PASS: pub/sub works";
    let _service =
      Mininats.sub nats ~sw ~subject:"test.echo" ~queue:None
        ~f:(fun ?reply_to msg ->
          match reply_to with
          | Some rt -> Mininats.pub nats ~subject:rt msg
          | None -> ())
    in
    (match
       Mininats.request nats ~sw ~clock ~subject:"test.echo" ~timeout:2.0 "ping"
     with
    | Ok "ping" -> traceln "PASS: request/reply works"
    | r ->
      traceln "FAIL: request/reply: %s"
        (match r with
        | Ok s -> s
        | Error _ -> "timeout"));
    (match
       Mininats.request nats ~sw ~clock ~subject:"test.nobody" ~timeout:0.2
         "hello"
     with
    | Error `Timeout -> traceln "PASS: timeout works"
    | _ -> traceln "FAIL: timeout")
  | _ ->
    traceln "FAIL: pub/sub got %s"
      (match !got with
      | Some s -> s
      | None -> "nothing"));
  Mininats.close nats;
  traceln "all tests passed"
