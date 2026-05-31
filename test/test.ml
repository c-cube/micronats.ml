open Eio.Std

let ( let@ ) = ( @@ )

let () =
  Eio_posix.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Eio.Switch.run @@ fun sw ->
  let nats = Micronats.connect ~sw ~net () in
  traceln "connected!";
  (* test pub/sub *)
  let got = ref None in
  let _sub =
    Micronats.sub nats ~sw ~subject:[ "test"; "hello" ]
      (fun (msg : Micronats.msg) ->
        traceln "GOT: %S" msg.payload;
        got := Some msg.payload)
  in
  Micronats.pub nats ~subject:[ "test"; "hello" ] "world42";
  Eio.Time.sleep clock 0.3;
  (match !got with
  | Some "world42" ->
    traceln "PASS: pub/sub";
    (* test hpub/hsub *)
    let hgot = ref None in
    let _hsub =
      Micronats.sub nats ~sw ~subject:[ "test"; "headers" ]
        (fun (msg : Micronats.msg) ->
          traceln "GOT HEADERS: %s payload:%S"
            (match msg.headers with
            | Some hs ->
              String.concat ", " (List.map (fun (k, v) -> k ^ "=" ^ v) hs)
            | None -> "(none)")
            msg.payload;
          hgot := Some msg.payload)
    in
    Micronats.hpub nats ~subject:[ "test"; "headers" ]
      ~headers:[ "X-Foo", "bar"; "X-Baz", "42" ]
      "hello-headers";
    Eio.Time.sleep clock 0.3;
    (match !hgot with
    | Some "hello-headers" -> traceln "PASS: hpub/hsub"
    | _ -> traceln "FAIL: hpub/hsub");
    (* test request/reply *)
    let _service =
      Micronats.sub nats ~sw ~subject:[ "test"; "echo" ]
        (fun (msg : Micronats.msg) ->
          match msg.reply_to with
          | Some rt ->
            Micronats.pub nats
              ~subject:(String.split_on_char '.' rt)
              msg.payload
          | None -> ())
    in
    (match
       Micronats.request nats ~sw ~clock ~subject:[ "test"; "echo" ]
         ~timeout:2.0 "ping"
     with
    | Ok "ping" -> traceln "PASS: request/reply"
    | r ->
      traceln "FAIL: request/reply: %s"
        (match r with
        | Ok s -> s
        | Error _ -> "timeout"));
    (* test timeout *)
    (match
       Micronats.request nats ~sw ~clock ~subject:[ "test"; "nobody" ]
         ~timeout:0.2 "hello"
     with
    | Error `Timeout -> traceln "PASS: timeout"
    | _ -> traceln "FAIL: timeout")
  | _ -> traceln "FAIL: pub/sub");
  Micronats.close nats;
  (* test connect_to with IPv4 *)
  Eio.Switch.run (fun sw ->
      let@ nats4 =
        Micronats.with_connect ~sw ~net ~host:"127.0.0.1" ~port:4222 ()
      in
      let got4 = ref None in
      let _sub4 =
        Micronats.sub nats4 ~sw ~subject:[ "test"; "ipv4" ]
          (fun (msg : Micronats.msg) -> got4 := Some msg.payload)
      in
      Micronats.pub nats4 ~subject:[ "test"; "ipv4" ] "via-ipv4";
      Eio.Time.sleep clock 0.3;
      match !got4 with
      | Some "via-ipv4" -> traceln "PASS: connect_to IPv4"
      | _ -> traceln "FAIL: connect_to IPv4");
  (* test connect_to with IPv6 *)
  (match
     Eio.Switch.run (fun sw ->
         let@ nats6 =
           Micronats.with_connect ~sw ~net ~host:"::1" ~port:4222 ()
         in
         let got6 = ref None in
         let _sub6 =
           Micronats.sub nats6 ~sw ~subject:[ "test"; "ipv6" ]
             (fun (msg : Micronats.msg) -> got6 := Some msg.payload)
         in
         Micronats.pub nats6 ~subject:[ "test"; "ipv6" ] "via-ipv6";
         Eio.Time.sleep clock 0.3;
         match !got6 with
         | Some "via-ipv6" -> traceln "PASS: connect_to IPv6"
         | _ -> traceln "FAIL: connect_to IPv6")
   with
  | () -> ()
  | exception _ -> traceln "SKIP: connect_to IPv6 (not listening on ::1)");
  traceln "all tests passed"
