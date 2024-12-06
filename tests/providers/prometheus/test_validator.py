from rated_exporter_sdk.providers.prometheus.validation import QueryValidator


def test_can_validate_query():
    query = """histogram_quantile(0.95, sum by(le, namespace) (rate(httpmetrics_handled_requests_histogram_bucket{type="http", service="rpcproxy", namespace!~"rpcproxy|rpc-.*"}[1m]))) * on(namespace) group_left(eth_network) (1*kube_namespace_labels{eth_network="testnet"})"""  # noqa: E501
    QueryValidator().validate_query(query)


def test_can_find_closing():
    expression = 'histogram_quantile(0.95, sum by(le, namespace) (rate(httpmetrics_handled_requests_histogram_bucket{type="http", service="rpcproxy", namespace!~"rpcproxy|rpc-.*"}[1m]))) * on(namespace) group_left(eth_network) (1*kube_namespace_labels{eth_network="testnet"})'  # noqa: E501
    assert 167 == QueryValidator.find_closing("(", ")", expression)
    assert expression[168:171] == " * "
