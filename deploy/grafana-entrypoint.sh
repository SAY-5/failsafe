#!/bin/sh
# The stock image's /run.sh still owns Grafana startup and provisioning.
set -eu

export GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
require_auth=${FAILSAFE_GRAFANA_REQUIRE_AUTH-0}
case "$require_auth" in
    0|1) ;;
    *)
        echo "FAILSAFE_GRAFANA_REQUIRE_AUTH must be 0 or 1" >&2
        exit 64
        ;;
esac
case "${FAILSAFE_GRAFANA_BIND_ADDRESS:-127.0.0.1}:$require_auth" in
    127.0.0.1:0|::1:0)
        export GF_AUTH_ANONYMOUS_ENABLED=true
        export GF_AUTH_BASIC_ENABLED=false
        export GF_AUTH_DISABLE_LOGIN_FORM=true
        # Keep initial organization creation, but never create a known admin password.
        password=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
        if [ "${#password}" -ne 64 ]; then
            echo "Unable to generate local Grafana initialization password" >&2
            exit 70
        fi
        export GF_SECURITY_ADMIN_PASSWORD="$password"
        ;;
    *)
        password=${GF_SECURITY_ADMIN_PASSWORD:-}
        if [ "${#password}" -lt 16 ]; then
            echo "Authenticated Grafana requires a configured password of at least 16 characters" >&2
            exit 64
        fi
        case "$password" in
            *[![:space:]]*) ;;
            *)
                echo "Authenticated Grafana requires a configured password, not whitespace" >&2
                exit 64
                ;;
        esac
        export GF_AUTH_ANONYMOUS_ENABLED=false
        export GF_AUTH_BASIC_ENABLED=true
        export GF_AUTH_DISABLE_LOGIN_FORM=false
        ;;
esac

exec "$@"
