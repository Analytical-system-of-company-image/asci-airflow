# Installing packages from Package Registry Gitlab repo
#  PACKACGE_ACQ is exmaple of secret from PACKAGE REGISTRY installing
if [[ -v PACKAGE_ACQ ]] && ! [[ -z "$PACKAGE_ACQ" ]]; then
    pip install dns-acquiring --extra-index-url https://__token__:${PACKAGE_ACQ}@ic-git.dns-shop.ru/api/v4/projects/90/packages/pypi/simple --constraint ./constraints.txt
fi &&
if [[ -v PACKAGE_ON_ACQ ]] && ! [[ -z "$PACKAGE_ON_ACQ" ]]; then
    pip install online_acquiring --extra-index-url https://__token__:${PACKAGE_ON_ACQ}@ic-git.dns-shop.ru/api/v4/projects/187/packages/pypi/simple --constraint ./constraints.txt
fi &&
if [[ -v PACKAGE_NETWORK_TRAFFIC ]] && ! [[ -z "$PACKAGE_NETWORK_TRAFFIC" ]]; then
    pip install network_traffic --extra-index-url https://__token__:${PACKAGE_NETWORK_TRAFFIC}@ic-git.dns-shop.ru/api/v4/projects/210/packages/pypi/simple --constraint ./constraints.txt
fi