# SYS-06 v2 upgrade

`v2_service.py` provides the safer dispatch core: a serialised state machine, per-notification quota accounting, FIFO delayed delivery, duplicate suppression while queued, and explicit outcomes. It is additive so the original thesis demo remains intact.

Run its validation with the local environment:

```powershell
E:\Graduation_Thesis\Test\venv\Scripts\python.exe -m pytest E:\Graduation_Thesis\SYS-06\tests -q
```

For production wiring, replace the current dispatcher import with `AlertDispatcherV2` and expose `/alerts` plus `/health` from the existing platform gateway. Keep credentials only in `.env`; never commit them or put them in an example file.
