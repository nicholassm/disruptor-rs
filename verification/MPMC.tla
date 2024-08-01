-------------------------------- MODULE MPMC --------------------------------
(***************************************************************************)
(*  Models a Multi Producer, Multi Consumer Disruptor (MPMC).              *)
(* The model verifies that no data races occur between the publishers      *)
(* and consumers and that all consumers eventually read all published      *)
(* values.                                                                 *)
(***************************************************************************)

EXTENDS Naturals, Integers, FiniteSets, Sequences

CONSTANTS
  Writers,          (* Writer/publisher thread ids.    *)
  Readers,          (* Reader/consumer  thread ids.    *)
  MaxPublished,     (* Max number of published events. *)
  Size,             (* Ringbuffer size.                *)
  NULL

VARIABLES
  ringbuffer,
  next_sequence,    (* Shared counter for claiming a sequence for a Writer. *)
  claimed_sequence, (* Claimed sequence by each Writer.                     *)
  published,        (* Encodes whether each slot is published.              *)
  read,             (* Read Cursors. One per Reader.                        *)
  consumed,         (* Sequence of all read events by the Reader.           *)
  pc                (* Program Counter of each Writer/Reader.               *)

vars == <<
  ringbuffer,
  next_sequence,
  claimed_sequence,
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
Access       == "Access"
Advance      == "Advance"

Transition(t, from, to) ==
  /\ pc[t] = from
  /\ pc'   = [ pc EXCEPT ![t] = to ]

Buffer       == INSTANCE RingBuffer

TypeInvariant ==
  /\ Buffer!TypeInvariant
  /\ next_sequence    \in Nat
  /\ claimed_sequence \in [ Writers                -> Int                 ]
  /\ published        \in [ 0..Size                -> Int                 ]
  /\ read             \in [ Readers                -> Int                 ]
  /\ consumed         \in [ Readers                -> Seq(Nat)            ]
  /\ pc               \in [ Writers \union Readers -> { Access, Advance } ]

Init ==
  /\ Buffer!Init
  /\ next_sequence    = 0
  /\ claimed_sequence = [ w \in Writers                |-> -1     ]
  /\ published        = [ i \in 0..Size                |-> -1     ]
  /\ read             = [ r \in Readers                |-> -1     ]
  /\ consumed         = [ r \in Readers                |-> << >>  ]
  /\ pc               = [ a \in Writers \union Readers |-> Access ]

Range(f) ==
  { f[x] : x \in DOMAIN(f) }

MinReadSequence ==
  CHOOSE min \in Range(read) : \A r \in Readers : min <= read[r]

IsPublished(sequence) ==
  LET index == Buffer!IndexOf(sequence)
  IN  published[index] = sequence

Publish(sequence) ==
  LET index == Buffer!IndexOf(sequence)
  IN  published' = [ published EXCEPT ![index] = sequence ]

(***************************************************************************)
(* Publisher Actions:                                                      *)
(***************************************************************************)

BeginWrite ==
  LET
    seq      == next_sequence
    index    == Buffer!IndexOf(seq)
    min_read == MinReadSequence
  IN
    \* Are we clear of all consumers? (Potentially a full cycle behind).
    /\ min_read >= seq - Size
    /\ seq < MaxPublished
    /\ \E w \in Writers :
      /\ claimed_sequence' = [ claimed_sequence EXCEPT ![w] = seq ]
      /\ next_sequence'    = seq + 1
      /\ Transition(w, Access, Advance)
      /\ Buffer!Write(index, w, seq)
    /\ UNCHANGED << consumed, published, read >>

EndWrite ==
  /\ \E w \in Writers :
    LET
      seq   == claimed_sequence[w]
      index == Buffer!IndexOf(seq)
    IN
      /\ Transition(w, Advance, Access)
      /\ Buffer!EndWrite(index, w)
      /\ Publish(seq)
  /\ UNCHANGED << claimed_sequence, next_sequence, consumed, read >>

(***************************************************************************)
(* Consumer Actions:                                                       *)
(***************************************************************************)

BeginRead ==
  /\ \E r \in Readers :
    LET
      next  == read[r] + 1
      index == Buffer!IndexOf(next)
    IN
      /\ IsPublished(next)
      /\ Transition(r, Access, Advance)
      /\ Buffer!BeginRead(index, r)
      \* Track what we read from the ringbuffer.
      /\ consumed' = [ consumed EXCEPT ![r] = Append(@, Buffer!Read(index)) ]
  /\ UNCHANGED << claimed_sequence, next_sequence, published, read >>

EndRead ==
  /\ \E r \in Readers :
    LET
      next  == read[r] + 1
      index == Buffer!IndexOf(next)
    IN
      /\ Transition(r, Advance, Access)
      /\ Buffer!EndRead(index, r)
      /\ read' = [ read EXCEPT ![r] = next ]
  /\ UNCHANGED << claimed_sequence, next_sequence, consumed, published >>

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
