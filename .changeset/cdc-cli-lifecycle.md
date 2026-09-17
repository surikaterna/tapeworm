---
"tapeworm_dispatcher_mdb_rmq": patch
---

Address redemeine-4tud.2 (parent redemeine-4tud; source redemeine-gqxm): isolate
the CLI operational repair from the core CDC migration.

Also ensure CLI initialization and teardown always attempt Mongo closure and signal
listener removal, including initialization errors and rejected dispatcher shutdown.
Signal shutdown now has one ten-second grace budget independent of pending work.
Only after the deadline and cleanup attempts may the CLI host exit 124; library
lifecycle code reports a typed timeout and observes late failures without exiting.
An already-issued Mongo checkpoint may still complete after process exit; restart
must use inspected durable state and retain stable-identity duplicate handling.
