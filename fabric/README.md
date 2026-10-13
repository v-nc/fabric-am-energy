# Fabric item definitions

This folder is connected to the Fabric workspace `am-energy-dev` through Fabric Git integration (branch `main`).
Fabric writes one folder per item here on every commit from the workspace. Change items in Fabric and commit from
there, or edit the text definitions here (for example a semantic model's TMDL) and update the workspace from Git.
Lakehouses store metadata only; their data stays in OneLake.

The Eventhouse's KQL database sits under `eh_am_energy.Eventhouse/.children/`; its `DatabaseSchema.kql` is Fabric's
export of what [`kql/setup.kql`](../kql/setup.kql) creates. The Activator item `act_am_energy` is deliberately not
committed: its definition stores the alert recipient's e-mail address. Its two rules are described in
[decisions 37 and 38](../docs/decisions.md#real-time-path).
