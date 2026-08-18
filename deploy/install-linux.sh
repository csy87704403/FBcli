#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "请使用 sudo 运行此安装脚本" >&2
  exit 1
fi

bundle_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
case $(uname -m) in
  x86_64|amd64) release_arch=amd64 ;;
  aarch64|arm64) release_arch=arm64 ;;
  *) echo "不支持的 CPU 架构: $(uname -m)" >&2; exit 1 ;;
esac

for file in \
  "freebuff-gateway-linux-${release_arch}" \
  "freebuff-headless-linux-${release_arch}" \
  "freebuff-login-linux-${release_arch}" \
  tree-sitter.wasm freebuff-gateway.service; do
  if [[ ! -f "${bundle_dir}/${file}" ]]; then
    echo "发布包缺少文件: ${file}" >&2
    exit 1
  fi
done

install_dir=/opt/freebuff-gateway
config_dir=/etc/freebuff-gateway
data_dir=/var/lib/freebuff-gateway

if ! id freebuff-gateway >/dev/null 2>&1; then
  useradd --system --home-dir "${data_dir}" --shell /usr/sbin/nologin freebuff-gateway
fi

install -d -m 0755 "${install_dir}" "${config_dir}"
install -d -o freebuff-gateway -g freebuff-gateway -m 0700 \
  "${data_dir}" "${data_dir}/accounts" "${data_dir}/default-account" \
  "${data_dir}/tmp" "${data_dir}/work"
install -m 0755 "${bundle_dir}/freebuff-gateway-linux-${release_arch}" "${install_dir}/freebuff-gateway"
install -m 0755 "${bundle_dir}/freebuff-headless-linux-${release_arch}" "${install_dir}/freebuff-headless"
install -m 0755 "${bundle_dir}/freebuff-login-linux-${release_arch}" "${install_dir}/freebuff-login"
install -m 0644 "${bundle_dir}/tree-sitter.wasm" "${install_dir}/tree-sitter.wasm"
install -m 0644 "${bundle_dir}/freebuff-gateway.service" /etc/systemd/system/freebuff-gateway.service

env_file="${config_dir}/gateway.env"
generated_password=""
if [[ ! -f "${env_file}" ]]; then
  generated_password=${FREEBUFF_ADMIN_PASSWORD:-$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')}
  admin_user=${FREEBUFF_ADMIN_USER:-admin}
  umask 077
  printf '%s\n' \
    "FREEBUFF_ADMIN_USER=${admin_user}" \
    "FREEBUFF_ADMIN_PASSWORD=${generated_password}" \
    "FREEBUFF_API_LISTEN=0.0.0.0:16882" \
    "FREEBUFF_ADMIN_LISTEN=127.0.0.1:16883" >"${env_file}"
fi

systemctl daemon-reload
systemctl enable --now freebuff-gateway.service

echo "Freebuff CLI Gateway 已启动"
echo "API: http://服务器IP:16882"
echo "后台仅监听本机: http://127.0.0.1:16883"
if [[ -n "${generated_password}" ]]; then
  echo "后台用户: ${admin_user}"
  echo "后台密码: ${generated_password}"
  echo "请立即保存密码；配置文件位于 ${env_file}"
else
  echo "保留了现有后台凭证: ${env_file}"
fi
