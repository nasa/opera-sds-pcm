from fabric.api import execute, roles
from sdscli.adapters.hysds.fabfile import (
    get_context,
    run,
    resolve_role,
    rm_rf,
    send_template,
    send_template_user_override,
    create_user_rules_index,
    create_grq_user_rules_index,
    send_hysds_ui_conf,
    build_hysds_ui,
    exists,
    mkdir,
    upload_template,
    cd,
    settings,
    get_user_files_path,
    copy,
    install_es_template,
    pip_install_with_req,
    ops_dir,
    ssh_opts,
    extra_opts,
    chmod,
)


#####################################
# add custom fabric functions below
#####################################


# NOTE: remove this function once sdscli is updated
def ensure_ssl(node_type):
    ctx = get_context(node_type)
    if node_type == "grq":
        common_name = ctx["GRQ_FQDN"]
    elif node_type == "mozart":
        common_name = ctx["MOZART_FQDN"]
    else:
        raise RuntimeError(f"Unknown node type: {node_type}")
    prompts = {
        "Enter pass phrase for server.key:": "hysds",
        "Enter pass phrase for server.key.org:": "hysds",
        "Verifying - Enter pass phrase for server.key:": "hysds",
    }
    if not exists("ssl/server.key") or not exists("ssl/server.pem"):
        mkdir("ssl", ctx["OPS_USER"], ctx["OPS_USER"])
        upload_template(
            "ssl_server.cnf",
            "ssl/server.cnf",
            use_jinja=True,
            context={"commonName": common_name},
            template_dir=get_user_files_path(),
        )
        with cd("ssl"):
            with settings(prompts=prompts):
                run("openssl genrsa -des3 -out server.key 1024", pty=False)
                run(
                    "OPENSSL_CONF=server.cnf openssl req -new -key server.key -out server.csr",
                    pty=False,
                )
                run("cp server.key server.key.org")
                run("openssl rsa -in server.key.org -out server.key", pty=False)
                run("chmod 600 server.key*")
                run(
                    "openssl x509 -req -days 99999 -in server.csr -signkey server.key -out server.pem",
                    pty=False,
                )


#####################################
# add custom fabric functions below
#####################################


def test():
    """Test fabric function."""

    run("whoami")


def copy_opera_pcm():
    role, hysds_dir, hostname = resolve_role()
    if role == "factotum" or role == "mozart":
        if role == "mozart":
            hysds_dir = "verdi"

        rm_rf("~/%s/ops/opera-pcm" % hysds_dir)
        copy("~/mozart/ops/opera-pcm", "~/%s/ops/" % hysds_dir)

        send_template(
            "settings.yaml",
            "%s/ops/opera-pcm/conf/settings.yaml" % hysds_dir,
            "~/mozart/ops/opera-pcm/conf",
        )

    if role == "mozart":
        hysds_dir = "verdi"
        rm_rf("~/%s/ops/opera-pcm" % hysds_dir)
        copy("~/mozart/ops/opera-pcm", "~/%s/ops/" % hysds_dir)


def copy_pcm_commons():
    role, hysds_dir, hostname = resolve_role()
    if role == "factotum":
        rm_rf("~/%s/ops/pcm_commons" % hysds_dir)
        copy("~/mozart/ops/pcm_commons", "~/%s/ops/" % hysds_dir)


def update_opera_packages():
    """Update verdi and factotum with OPERA packages."""

    role, hysds_dir, hostname = resolve_role()
    ctx = get_context(role)
    metrics_es_engine = ctx.get("METRICS_ES_ENGINE", "elasticsearch")

    if role == "mozart":
        rm_rf("~/.sds/rules/staging_rules.json")
        send_template(
            "user_rules-cnm.json.tmpl",
            "~/.sds/rules/user_rules-cnm.json",
            "~/mozart/ops/opera-pcm/conf/sds/rules",
        )
        # Copy the dashboard script template from ~/.sds/files to the deployment
        # area
        if metrics_es_engine == "opensearch":
            dashboard_dir = "opensearch_dashboards_import"
        else:
            dashboard_dir = "kibana_dashboard_import"
        copy(
            f"~/.sds/files/{dashboard_dir}/import_dashboard.sh.tmpl",
            f"~/mozart/ops/sdscli/sdscli/adapters/hysds/files/{dashboard_dir}/import_dashboard.sh.tmpl",
        )
        copy(
            "~/mozart/ops/pcm_commons/pcm_commons/tools/snapshot_es_data.py",
            f"{hysds_dir}/bin/snapshot_es_data.py",
        )
        run(f"chmod +x {hysds_dir}/bin/snapshot_es_data.py")
        copy(
            "~/mozart/ops/opera-pcm/cluster_provisioning/restore_snapshot.sh",
            "%s/bin/restore_snapshot.sh" % hysds_dir,
        )
        run(f"chmod +x {hysds_dir}/bin/restore_snapshot.sh")


    if role == "grq":
        update_run_aws_es_sh()

    if role == "metrics":
        run_sds_watch_using_local_logstash_installation()

    if role != "grq":
        update_opera_pcm_settings()
        update_harikiri_config()
        update_spot_termination_config()

    hysds_dirs = get_hysds_dirs()
    for hysds_dir in hysds_dirs:
        if hysds_dir == "verdi":
            send_template(
                "docker-compose.yml.verdi",
                "~/verdi/ops/hysds-dockerfiles/verdi/docker-compose.yml",
            )
            send_template(
                "run_verdi_podman.sh.tmpl",
                "~/verdi/ops/hysds-dockerfiles/verdi/run_verdi_podman.sh",
            )
            # Deploying this will allow us to dynamically determine whether we should start up verdi
            # with docker or podman
            send_template(
                "start-verdi.sh",
                "~/verdi/ops/hysds-dockerfiles/verdi/start-verdi.sh",
            )
            execute(chmod, "uog+x", "~/verdi/ops/hysds-dockerfiles/verdi/start-verdi.sh", roles=[role])



def get_hysds_dirs():
    role, hysds_dir, _ = resolve_role()
    if role == "mozart":
        hysds_dirs = [hysds_dir, "verdi"]
    else:
        hysds_dirs = [hysds_dir]
    return hysds_dirs


def update_run_aws_es_sh():
    role, _, _ = resolve_role()
    if role == "grq":
        hysds_dirs = get_hysds_dirs()
        for hysds_dir in hysds_dirs:
            rm_rf(f"{hysds_dir}/bin/run_aws_es.sh")
            send_template("run_aws_es.sh", f"{hysds_dir}/bin/run_aws_es.sh")
            run(f"chmod 755 {hysds_dir}/bin/run_aws_es.sh")
            break


def run_sds_watch_using_local_logstash_installation():
    role, _, _ = resolve_role()
    if role == "metrics":
        hysds_dirs = get_hysds_dirs()
        for hysds_dir in hysds_dirs:
            run(f'sed -i "s#/sdswatch/#${{HOME}}/{hysds_dir}/#" ~/metrics/etc/sdswatch_client.conf')


def update_opera_pcm_settings():
    role, _, _ = resolve_role()
    hysds_dirs = get_hysds_dirs()
    if role != "grq":
        for hysds_dir in hysds_dirs:
            rm_rf(f"{hysds_dir}/etc/settings.yaml")
            send_template(
                "settings.yaml",
                f"{hysds_dir}/etc/settings.yaml",
                "~/mozart/ops/opera-pcm/conf",
            )


def update_harikiri_config():
    role, _, _ = resolve_role()
    hysds_dirs = get_hysds_dirs()
    if role != "grq":
        for hysds_dir in hysds_dirs:
            rm_rf(f"{hysds_dir}/etc/harikiri.yml")
            send_template(
                "harikiri.yml.tmpl",
                f"{hysds_dir}/etc/harikiri.yml"
            )


def update_spot_termination_config():
    role, _, _ = resolve_role()
    hysds_dirs = get_hysds_dirs()
    if role != "grq":
        for hysds_dir in hysds_dirs:
            rm_rf("%s/etc/spot_termination_detector.yml" % hysds_dir)
            send_template(
                "spot_termination_detector.yml.tmpl",
                f"{hysds_dir}/etc/spot_termination_detector.yml"
            )


def update_celery_config():
    role, hysds_dir, _ = resolve_role()
    if role == 'mozart':
        hysds_dirs = [hysds_dir, "verdi"]
        for hysds_dir in hysds_dirs:
            if hysds_dir == "verdi":
                send_template_user_override("celeryconfig.py.tmpl.private_verdi",
                                            "~/%s/ops/hysds/celeryconfig.py" % hysds_dir, role)


def deploy_hysds_ui():
    role, _, _ = resolve_role()
    if role == "mozart":
        send_hysds_ui_conf()
        build_hysds_ui()


def create_all_user_rules_index():
    """Create user_rules index on grq."""
    role, hysds_dir, _ = resolve_role()
    if role == "grq":
        send_template(
            "user_rules_dataset.mapping",
            f"{hysds_dir}/ops/grq2/config/user_rules_dataset.mapping",
        )
        create_grq_user_rules_index()
    if role == "mozart":
        send_template(
            "user_rules_job.mapping",
            f"{hysds_dir}/ops/mozart/configs/user_rules_job.mapping",
        )
        create_user_rules_index()


@roles("mozart")
def update_ilm_policy_mozart():
    role, hysds_dir, hostname = resolve_role()
    context = get_context()
    mozart_es_engine = context.get("MOZART_ES_ENGINE", "elasticsearch")

    if role == "mozart":
        if mozart_es_engine == "opensearch":
            policy_file_name = "opensearch_ism_policy_mozart.json"
        else:
            policy_file_name = "es_ilm_policy_mozart.json"

    #copy(
    #    f"~/.sds/files/{policy_file_name}",
    #    f"{hysds_dir}/ops/grq2/config/{policy_file_name}"
    #)
    #run(
    #    "curl --request PUT --url 'localhost:9200/_ilm/policy/ilm_policy_mozart?pretty' "
    #    "--fail-with-body "
    #    f"--json @{hysds_dir}/ops/grq2/config/{policy_file_name}"
    #)

    rm_rf(f"~/.sds/files/{policy_file_name}")
    send_template(
        f"{policy_file_name}.tmpl",
        f"~/.sds/files/{policy_file_name}",
        "~/mozart/ops/opera-pcm/conf/sds/files",
    )


@roles("grq")
def update_grq_es():
    context = get_context()
    grq_es_engine = context.get("GRQ_ES_ENGINE", "elasticsearch")
    if grq_es_engine == "opensearch":
        create_ism_policy_grq()
        # TODO chrisjrd: implement default index template overrides here
        override_os_grq_default_index_template()
        create_os_index_templates_grq()
    elif grq_es_engine == "elasticsearch":
        create_ilm_policy_grq()
        override_grq_default_index_template()
        create_index_templates_grq()


@roles("grq")
def create_ism_policy_grq():
    _, hysds_dir, _ = resolve_role()

    send_template(
        "os_ism_policy_grq.json",
        f"{hysds_dir}/ops/grq2/config/os_ism_policy_grq.json",
        tmpl_dir="~/.sds/files/opensearch/"
    )
    run(
        f"curl --request PUT --url 'localhost:9200/_plugins/_ism/policies/opera_grq_ism_policy?pretty' "
        "--fail-with-body "
        f"--json @{hysds_dir}/ops/grq2/config/os_ism_policy_grq.json")


@roles("grq")
def create_ilm_policy_grq():
    _, hysds_dir, _ = resolve_role()

    for file, policy in [
        ("es_ilm_policy_grq.json", "opera_grq_ilm_policy"),
        ("es_ilm_policy_grq_c_cslc.json", "opera_grq_c_cslc_ilm_policy")]:

        copy(
            f"~/.sds/files/elasticsearch/{file}",
            f"{hysds_dir}/ops/grq2/config/{file}"
        )
        run(
            f"curl --request PUT --url 'localhost:9200/_ilm/policy/{policy}?pretty' "
            "--fail-with-body "
            f"--json @{hysds_dir}/ops/grq2/config/{file}"
        )


@roles("grq")
def override_grq_default_index_template():
    role, hysds_dir, _ = resolve_role()

    copy(
        "~/.sds/files/es_template.json",
        f"{hysds_dir}/ops/grq2/config/es_template.json",
    )
    execute(install_es_template, roles=[role])


@roles("grq")
def override_os_grq_default_index_template():
    role, hysds_dir, _ = resolve_role()

    copy(
        "~/.sds/files/os_template.json",
        f"{hysds_dir}/ops/grq2/config/es_template.json",
    )
    execute(install_es_template, roles=[role])


@roles("grq")
def create_index_templates_grq():
    role, hysds_dir, _ = resolve_role()

    print(f"Creating index templates for {role}")

    for file, template in [
        ("es_template_jobs_accountability_catalog.json",    "jobs_accountability_catalog_template"),
        ("es_template_hls_catalog.json",                    "hls_catalog_template"),
        ("es_template_hls_spatial_catalog.json",            "hls_spatial_catalog_template"),
        ("es_template_slc_catalog.json",                    "slc_catalog_template"),
        ("es_template_slc_spatial_catalog.json",            "slc_spatial_catalog_template"),
        ("es_template_rtc_catalog.json",                    "rtc_catalog_template"),
        ("es_template_cslc_catalog.json",                   "cslc_catalog_template"),
        ("es_template_k_cslc_catalog.json",                 "k_cslc_catalog_template"),
        ("es_template_cslc_compressed_product.json",        "cslc_compressed_product_template")
    ]:
        copy(
            f"~/.sds/files/elasticsearch/grq_es_templates/{file}",
            f"{hysds_dir}/ops/grq2/config/{file}"
        )
        run(
            f"curl --request PUT --url 'localhost:9200/_index_template/{template}?pretty&create=true' "
            "--fail-with-body "
            f"--json @{hysds_dir}/ops/grq2/config/{file}"
        )


@roles("grq")
def create_os_index_templates_grq():
    role, hysds_dir, _ = resolve_role()

    print(f"Creating index templates for {role}")

    for file, template in [
        ("os_template_jobs_accountability_catalog.json",    "jobs_accountability_catalog_template"),
        ("os_template_hls_catalog.json",                    "hls_catalog_template"),
        ("os_template_hls_spatial_catalog.json",            "hls_spatial_catalog_template"),
        ("os_template_slc_catalog.json",                    "slc_catalog_template"),
        ("os_template_slc_spatial_catalog.json",            "slc_spatial_catalog_template"),
        ("os_template_rtc_catalog.json",                    "rtc_catalog_template"),
        ("os_template_cslc_catalog.json",                   "cslc_catalog_template"),
        ("os_template_k_cslc_catalog.json",                 "k_cslc_catalog_template"),
        ("os_template_cslc_compressed_product.json",        "cslc_compressed_product_template")
    ]:
        copy(
            f"~/.sds/files/opensearch/grq_os_templates/{file}",
            f"{hysds_dir}/ops/grq2/config/{file}"
        )
        run(
            f"curl --request PUT --url 'localhost:9200/_index_template/{template}?pretty&create=true' "
            "--fail-with-body "
            f"--json @{hysds_dir}/ops/grq2/config/{file}"
        )

@roles("metrics")
def update_metrics_es():
    _, hysds_dir, _ = resolve_role()

    # Need to create this directory first as it does not exist
    context = get_context()
    mkdir(f"{hysds_dir}/ops/metrics/config", context['OPS_USER'], context['OPS_USER'])

    metrics_es_engine = context.get("METRICS_ES_ENGINE", "elasticsearch")
    if metrics_es_engine == "opensearch":
        create_ism_policy_metrics()
        create_os_index_templates_metrics()
    elif metrics_es_engine == "elasticsearch":
        create_ilm_policy_metrics()
        create_index_templates_metrics()

@roles("metrics")
def create_ism_policy_metrics():
    _, hysds_dir, _ = resolve_role()

    send_template(
        "os_ism_policy_metrics.json",
        f"{hysds_dir}/ops/metrics/config/os_ism_policy_metrics.json",
        tmpl_dir="~/.sds/files/opensearch/"
    )
    run(
        f"curl --request PUT --url 'localhost:9200/_plugins/_ism/policies/opera_metrics_ism_policy?pretty' "
        "--fail-with-body "
        f"--json @{hysds_dir}/ops/metrics/config/os_ism_policy_metrics.json")

@roles("metrics")
def create_os_index_templates_metrics():
    _, hysds_dir, _ = resolve_role()

    send_template(
        "os_template_metrics.json",
        f"{hysds_dir}/ops/metrics/config/os_template_metrics.json",
        tmpl_dir="~/.sds/files/opensearch/"
    )
    run(f"curl --request PUT --url 'localhost:9200/_index_template/metrics_index_template?pretty' "
        "--fail-with-body "
        f"--json @{hysds_dir}/ops/metrics/config/os_template_metrics.json")

    send_template(
        "os_template_metrics-logstash.json",
        f"{hysds_dir}/ops/metrics/config/os_template_metrics-logstash.json",
        tmpl_dir="~/.sds/files/opensearch/"
    )
    run(f"curl --request PUT --url 'localhost:9200/_index_template/logstash_template?pretty' "
        "--fail-with-body "
        f"--json @{hysds_dir}/ops/metrics/config/os_template_metrics-logstash.json")


@roles("metrics")
def create_ilm_policy_metrics():
    _, hysds_dir, _ = resolve_role()

    send_template(
        "es_ilm_policy_metrics.json",
        f"{hysds_dir}/ops/metrics/config/es_ilm_policy_metrics.json",
        tmpl_dir="~/.sds/files/elasticsearch/"
    )
    run(
        "curl --request PUT --url 'localhost:9200/_ilm/policy/opera_metrics_ilm_policy?pretty' "
        "--fail-with-body "
        f"--json @{hysds_dir}/ops/metrics/config/es_ilm_policy_metrics.json"
    )


@roles("metrics")
def create_index_templates_metrics():
    _, hysds_dir, _ = resolve_role()

    send_template(
        "es_template_metrics.json",
        f"{hysds_dir}/ops/metrics/config/es_template_metrics.json",
        tmpl_dir="~/.sds/files/elasticsearch/"
    )
    run(
        "curl --request PUT --url 'localhost:9200/_index_template/metrics_index_template?pretty&create=true' "
        "--fail-with-body "
        f"--json @{hysds_dir}/ops/metrics/config/es_template_metrics.json"
    )
    send_template(
        "es_template_metrics-logstash.json",
        f"{hysds_dir}/ops/metrics/config/es_template_metrics-logstash.json",
        tmpl_dir="~/.sds/files/elasticsearch/"
    )
    run(
        "curl --request PUT --url 'localhost:9200/_index_template/logstash_template?pretty&create=true' "
        "--fail-with-body "
        f"--json @{hysds_dir}/ops/metrics/config/es_template_metrics-logstash.json"
    )


def load_container_in_registry(container_name):
    role, _, _ = resolve_role()
    ctx = get_context(role)
    if role == 'mozart':
        run(f"aws s3 cp s3://{ctx['CODE_BUCKET']}/{container_name}.tar.gz ~/mozart/pkgs/")
        run(f"docker load < ~/mozart/pkgs/{container_name}.tar.gz")
        run(f"docker tag {container_name} {ctx['CONTAINER_REGISTRY']}/{container_name}")
        run(f"docker push {ctx['CONTAINER_REGISTRY']}/{container_name}")
