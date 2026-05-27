(lang dune 3.0)
(name mininats)
(version 0.1)
(generate_opam_files true)

(package
 (name mininats)
 (synopsis "Minimal NATS client library using Eio")
 (depends
  (ocaml (>= 5.0))
  dune
  eio))
