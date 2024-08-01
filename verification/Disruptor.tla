----------------------------- MODULE Disruptor -----------------------------
(***************************************************************************)
(* Models a Single Producer, Multi Consumer Disruptor (SPMC).              *)
(*                                                                         *)
(* The model verifies that no data races occur between the publisher       *)
(* and consumers and that all consumers eventually read all published      *)
(* values.                                                                 *)
(*                                                                         *)
(* To see a data race, try and run the model with two publishers.          *)
(***************************************************************************)

EXTENDS Naturals, Integers, FiniteSets, Sequences

CONSTANTS
  Writers,      (* Writer/publisher thread ids.    *)
  Readers,      (* Reader/consumer  thread ids.    *)
  MaxPublished, (* Max number of published events. *)
  Size,         (* Ringbuffer size.                *)
  NULL

VARIABLES
  ringbuffer,
  published,    (* Publisher Cursor.                           *)
  read,         (* Read Cursors. One per consumer.             *)
  consumed,     (* Sequence of all read events by the Readers. *)
  pc            (* Program Counter of each Writer/Reader.      *)

vars == <<
  ringbuffer,
  published,
  read,
  consumed,
  pc
>>

(***************************************************************************)
(* Each publisher/consumer can be in one of two states:                    *)
(* 1. Accessing a slot in the Disruptor or                                 *)
(* 2. Advancing to the next slot.                                          *)
(***************************************************************************)
Access  == "Access"
Advance == "Advance"

Transition(t, from, to) ==
  /\ pc[t] = from
  /\ pc'   = [ pc EXCEPT ![t] = to ]

Buffer  == INSTANCE RingBuffer

TypeInvariant ==
  /\ Buffer!TypeInvariant
  /\ published \in Int
  /\ read      \in [ Readers                -> Int                 ]
  /\ consumed  \in [ Readers                -> Seq(Nat)            ]
  /\ pc        \in [ Writers \union Readers -> { Access, Advance } ]

Init ==
  /\ Buffer!Init
  /\ published = -1
  /\ read      = [ r \in Readers                |-> -1     ]
  /\ consumed  = [ r \in Readers                |-> << >>  ]
  /\ pc        = [ a \in Writers \union Readers |-> Access ]

Range(f) ==
  { f[x] : x \in DOMAIN(f) }

MinReadSequence ==
  CHOOSE min \in Range(read) : \A r \in Readers : min <= read[r]

(***************************************************************************)
(* Publisher Actions:                                                      *)
(***************************************************************************)

BeginWrite ==
  LET
    next     == published + 1
    index    == Buffer!IndexOf(next)
    min_read == MinReadSequence
  IN
    \* Are we clear of all consumers? (Potentially a full cycle behind).
    /\ min_read >= next - Size
    /\ next < MaxPublished
    /\ \E w \in Writers :
      /\ Transition(w, Access, Advance)
      /\ Buffer!Write(index, w, next)
    /\ UNCHANGED << consumed, published, read >>

EndWrite ==
  LET
    next  == published + 1
    index == Buffer!IndexOf(next)
  IN
    /\ \E w \in Writers :
      /\ Transition(w, Advance, Access)
      /\ Buffer!EndWrite(index, w)
      /\ published' = next
    /\ UNCHANGED << consumed, read >>

(***************************************************************************)
(* Consumer Actions:                                                       *)
(***************************************************************************)

BeginRead ==
  /\ \E r \in Readers :
    LET
      next  == read[r] + 1
      index == Buffer!IndexOf(next)
    IN
      /\ published >= next
      /\ Transition(r, Access, Advance)
      /\ Buffer!BeginRead(index, r)
      \* Track what we read from the ringbuffer.
      /\ consumed' = [ consumed EXCEPT ![r] = Append(@, Buffer!Read(index)) ]
  /\ UNCHANGED << published, read >>

EndRead ==
  /\ \E r \in Readers :
    LET
      next  == read[r] + 1
      index == Buffer!IndexOf(next)
    IN
      /\ Transition(r, Advance, Access)
      /\ Buffer!EndRead(index, r)
      /\ read' = [ read EXCEPT ![r] = next ]
  /\ UNCHANGED << consumed, published >>

(***************************************************************************)
(* Spec:                                                                   *)
(***************************************************************************)

Next ==
  \/ BeginWrite
  \/ EndWrite
  \/ BeginRead
  \/ EndRead

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ WF_vars(BeginWrite)
  /\ WF_vars(EndWrite)
  /\ WF_vars(BeginRead)
  /\ WF_vars(EndRead)

-----------------------------------------------------------------------------

NoDataRaces == Buffer!NoDataRaces

Liveliness ==
  <>[] (\A r \in Readers : consumed[r] = [i \in 1..MaxPublished |-> i - 1])

=============================================================================
