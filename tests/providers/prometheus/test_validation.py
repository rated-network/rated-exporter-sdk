from rated_exporter_sdk.providers.prometheus import validation

VALID_QUERY = 'histogram_quantile(0.95, sum by(le, namespace) (rate(httpmetrics_handled_requests_histogram_bucket{type="http", service="rpcproxy", namespace!~"rpcproxy|rpc-.*"}[1m]))) * on(namespace) group_left(eth_network) (1*kube_namespace_labels{eth_network="testnet"})'


def test_validate_query():
    validation.QueryValidator().validate_query(VALID_QUERY)


def test_check_balanced():
    assert validation.QueryValidator().check_balanced(VALID_QUERY) is None


def test_tokenize_query():
    tokens = validation.QueryValidator().tokenize_query(VALID_QUERY)
    expected = ["histogram_quantile", "..."]  # Not sure what is expected yet

    assert tokens == expected
