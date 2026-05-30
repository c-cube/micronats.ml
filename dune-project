(lang dune 3.0)
(name micronats)
(version 0.1)
(generate_opam_files true)

(package
 (name micronats)
 (synopsis "Micro NATS client library using Eio")
 (depends
  (ocaml (>= 5.0))
  dune
  (eio (>= 1.0))
  (eio_posix :with-test)))
