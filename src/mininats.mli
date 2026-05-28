(** Minimal NATS client using Eio.

    Connect to a NATS server, publish, subscribe, and make requests. Supports
    plain messages and messages with headers (HPUB/HMSG). The connection lives
    on a switch; close it by calling {!close} or finishing the switch. *)

(** {2 Types} *)

type t
(** A connection to a NATS server. *)

type sub
(** A subscription handle. *)

type header = string * string
(** A header key-value pair. *)

(** {2 Connection} *)

val connect :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  ?token:string ->
  ?user:string ->
  ?pass:string ->
  unit ->
  t
(** Connect to [localhost:4222]. *)

val connect_to :
  sw:Eio.Switch.t ->
  net:_ Eio.Net.t ->
  ?token:string ->
  ?user:string ->
  ?pass:string ->
  host:string ->
  port:int ->
  unit ->
  t
(** Connect to a NATS server at [host]:[port]. *)

(** {2 Messaging} *)

val pub : t -> subject:string -> ?reply_to:string -> string -> unit
(** Publish a plain message. *)

val hpub :
  t ->
  subject:string ->
  ?reply_to:string ->
  ?headers:header list ->
  string ->
  unit
(** Publish a message with headers. *)

val sub :
  t ->
  sw:Eio.Switch.t ->
  subject:string ->
  queue:string option ->
  f:(?reply_to:string -> ?headers:header list -> string -> unit) ->
  sub
(** Subscribe to [subject] with an optional [queue] group. The callback receives
    optional reply-to, optional headers, and the payload. Auto-unsubscribed when
    [sw] finishes. *)

val unsub : t -> max_msgs:int option -> sub -> unit
(** Explicitly unsubscribe. *)

val request :
  t ->
  sw:Eio.Switch.t ->
  clock:_ Eio.Time.clock ->
  subject:string ->
  timeout:float ->
  string ->
  (string, [> `Timeout ]) result
(** Send a request, wait for a single reply up to [timeout] seconds. *)

(** {2 Lifecycle} *)

val close : t -> unit
(** Shut down the underlying socket, causing the reader fiber to exit. *)

(** {2 Retry} *)

val with_retry :
  clock:_ Eio.Time.clock ->
  ?delay:float ->
  ?max_retries:int ->
  connect:(unit -> t) ->
  unit ->
  (t -> 'a) ->
  'a
(** [with_retry ~clock ~delay ~max_retries ~connect () f] calls [connect]
    repeatedly on failure, sleeping [delay] seconds between attempts. If
    [max_retries] is [Some n], gives up after [n] retries. The connection is
    automatically closed after [f] returns.

    Can be used with [let@ conn = with_retry ~clock ~connect () in ....] as
    well. *)
