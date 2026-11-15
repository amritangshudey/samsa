# Soak: FAIL

24h0m0s, 500 records/s of 1 KiB, retention 256 MiB, darwin/arm64, Go go1.27.3.

| | min | median | max | last |
|---|---|---|---|---|
| RSS, MiB | 14 | 76 | 173 | 115 |
| data directory, MiB | 29 | 224 | 256 | 249 |
| open files | 16 | 21 | 27 | 16 |
| client connections | 5 | 5 | 12 | 5 |

Produced 43060729, consumed 43060729, produce errors 0, gaps 0. Restart on the soaked data: ready in 182ms.

- FAIL: memory grows: peak RSS 109 MiB in the second quarter, 173 MiB in the last
