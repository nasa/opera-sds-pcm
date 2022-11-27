import os

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
    extra_opts
)

from fabric.api import execute
from fabric.contrib.project import rsync_project

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


def update_opera_packages():
    """Update verdi and factotum with OPERA packages."""

    role, hysds_dir, hostname = resolve_role()

    if role == "mozart":
        rm_rf("~/.sds/rules/staging_rules.json")
        send_template(
            "user_rules-cnm.json.tmpl",
            "~/.sds/rules/user_rules-cnm.json",
            "~/mozart/ops/opera-pcm/conf/sds/rules",
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
        update_bach_api()

    if role == "metrics":
        run_sds_watch_using_local_logstash_installation()

    if role != "grq":
        update_opera_pcm_settings()
        update_harikiri_config()
        update_spot_termination_config()


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


def update_bach_api():
    role, _, _ = resolve_role()
    if role == "grq":
        hysds_dirs = get_hysds_dirs()
        for hysds_dir in hysds_dirs:
            rm_rf(f'{hysds_dir}/ops/bach-api')
            rsync_project(f'{hysds_dir}/ops/', os.path.join(ops_dir, 'mozart/ops/bach-api'),
                          extra_opts=extra_opts, ssh_opts=ssh_opts)
            execute(pip_install_with_req, 'sciflo',
                    '~/sciflo/ops/bach-api', False, roles=[role])

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


def update_es_template():
    # Overwrites the default ES template with NISAR's custom one
    role, hysds_dir, _ = resolve_role()

    if role == 'grq':
        copy(
            "~/.sds/files/es_template.json",
            f"{hysds_dir}/ops/grq2/config/es_template.json",
        )
        execute(install_es_template, roles=[role])


def load_container_in_registry(container_name):
    role, _, _ = resolve_role()
    ctx = get_context(role)
    if role == 'mozart':
        run(f"aws s3 cp s3://{ctx['CODE_BUCKET']}/{container_name}.tar.gz ~/mozart/pkgs/")
        run(f"docker load < ~/mozart/pkgs/{container_name}.tar.gz")
        run(f"docker tag {container_name} {ctx['CONTAINER_REGISTRY']}/{container_name}")
        run(f"docker push {ctx['CONTAINER_REGISTRY']}/{container_name}")
