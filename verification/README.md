# Verification in TLA+

This folder contains two models for verifying the SPMC and MPMC scenarios.

To run the verifications in less than a minute, these model configurations can be used:

**SPMC:**

- MaxPublished <- 10
- Size <- 8
- Writers <- { "w" }
- Readers <- { "r1", "r2" }
- NULL <- [ model value ]

**MPMC:**

- MaxPublished <- 10
- Size <- 8
- Writers <- { "w1", "w2" }
- Readers <- { "r1", "r2" }
- NULL <- [ model value ]
