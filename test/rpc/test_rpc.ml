open Eio.Std
open Calculator

let server () : Micronats_rpc.handler Pbrt_services.Server.t =
  Calc.Server.make
    ~add:(fun rpc ->
      Micronats_rpc.mk_h_one rpc (fun (req : add_req) ->
          make_add_res ~result:(Int32.add req.a req.b) ()))
    ~sub:(fun rpc ->
      Micronats_rpc.mk_h_one rpc (fun (req : sub_req) ->
          make_sub_res ~result:(Int32.sub req.a req.b) ()))
    ~mul:(fun rpc ->
      Micronats_rpc.mk_h_one rpc (fun (req : mul_req) ->
          make_mul_res ~result:(Int32.mul req.a req.b) ()))
    ()

let () =
  Eio_posix.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Eio.Switch.run @@ fun sw ->
  let nats_a = Micronats.connect ~sw ~net () in
  let nats_b = Micronats.connect ~sw ~net () in
  let srv = server () in
  Micronats_rpc.add_service nats_a ~sw srv;
  Micronats_rpc.add_service nats_b ~sw srv;
  Eio.Time.sleep clock 0.3;
  (* conn_a calls Add *)
  let res =
    Micronats_rpc.send_request nats_a ~sw ~clock Calc.Client.add
      (make_add_req ~a:2l ~b:3l ())
    |> Eio.Promise.await_exn
  in
  traceln "conn_a: Add(2,3) = %ld" res.result;
  assert (res.result = 5l);
  (* conn_b calls Sub *)
  let res =
    Micronats_rpc.send_request nats_b ~sw ~clock Calc.Client.sub
      (make_sub_req ~a:10l ~b:4l ())
    |> Eio.Promise.await_exn
  in
  traceln "conn_b: Sub(10,4) = %ld" res.result;
  assert (res.result = 6l);
  (* conn_a calls Mul *)
  let res =
    Micronats_rpc.send_request nats_a ~sw ~clock Calc.Client.mul
      (make_mul_req ~a:6l ~b:7l ())
    |> Eio.Promise.await_exn
  in
  traceln "conn_a: Mul(6,7) = %ld" res.result;
  assert (res.result = 42l);
  (* conn_b calls Add *)
  let res =
    Micronats_rpc.send_request nats_b ~sw ~clock Calc.Client.add
      (make_add_req ~a:1l ~b:1l ())
    |> Eio.Promise.await_exn
  in
  traceln "conn_b: Add(1,1) = %ld" res.result;
  assert (res.result = 2l);
  traceln "PASS: rpc";
  Micronats.close nats_a;
  Micronats.close nats_b
