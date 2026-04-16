# Options Automation TODO

Status: active

As of: Thursday, April 16, 2026

## Next Steps

- [ ] Prove the new `call_credit_spread` lane through a full paper lifecycle: opportunity, decision, intent, submit, fill, position creation, management, and close.
- [ ] Tune selection thresholds, sizing, and risk caps for both short-dated credit bots using the new automation analytics and live paper observations.
- [ ] Define the paper-to-live promotion checklist for each bot, including explicit go/no-go criteria, manual approval expectations, and required safety limits.
- [ ] Make `iron_condor` runtime-ready by adding generic multi-leg live deployment validation, condor-aware exposure math, and condor-aware risk fallback / fail-closed behavior.
- [ ] Prove the `iron_condor` lane through a full paper lifecycle once the shared runtime gaps are fixed.
- [ ] Design canonical support for naked short calls/puts, including family modeling, undefined-risk controls, margin-aware sizing, and a separate live validation path.
- [ ] Add butterfly family support end to end: canonical family modeling, builder/runtime support, exposure math, and lifecycle validation.
