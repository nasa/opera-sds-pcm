To use template files (.tmpl by convention):
1. define parameter key-value pairs in sds/config / settings.yaml (or populate them in main.tf)
2. reference them in the template file
3. in cluster.py, read parameters from context and pass into template
   ```python
   from sdscli.adapters.hysds.fabfile import get_context, upload_template
   context = get_context()
   my_param = context["my_param"]  # read from ~/.sds/config (settings.yaml)
   ...
   upload_template(..., use_jinja=True,
     context={"my_param": my_param}
   )
   ```