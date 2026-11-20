STATE CONFIG TOOLS (DIST-S1) INSTRUCTIONS
---

install dependencies from project root.
```bash
#!/bin/env bash

pip install -e '.[dist_s1_tools]'
```

Set environment variables `GRQ_USER` and `GRQ_PASSWORD`.
`.env` file supported.
```dotenv
GRQ_USER=#<HYSDS UI username>
GRQ_PASSWORD=#<HYSDS UI password>
```

Run using python from project root. See respective tool's `--help` for usage information.
    Examples:
```bash
#!/bin/env bash

python dist_s1/tools/validation/state_config_inspection/state_config_export --help
python dist_s1/tools/validation/state_config_inspection/state_config_audit_forward --help
python dist_s1/tools/validation/state_config_inspection/state_config_audit_historical --help
```