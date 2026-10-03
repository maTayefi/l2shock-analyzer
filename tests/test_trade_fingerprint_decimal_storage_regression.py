from __future__ import annotations

from decimal import Context, Decimal, localcontext

import pytest

import l2shock.price.hourly as hourly_module


def _fingerprint(
    *,
    price: Decimal,
    quantity: Decimal,
    symbol: str = "BTCUSDT",
    received_time_ns: int = 1_000_000_000,
    event_time_ms: int = 1_000,
    trade_time_ms: int = 1_000,
    is_buyer_maker: bool = False,
    order_type: str | None = "market",
) -> hourly_module.TradeFingerprint:
    return (
        symbol,
        price,
        quantity,
        received_time_ns,
        event_time_ms,
        trade_time_ms,
        is_buyer_maker,
        order_type,
    )


@pytest.mark.parametrize(
    ("price_text", "quantity_text"),
    (
        ("1E+1000000000", "1E-1000000000"),
        ("1E-1000000000", "1E+1000000000"),
    ),
)
def test_fingerprint_storage_does_not_expand_decimal_exponents(
    monkeypatch: pytest.MonkeyPatch,
    price_text: str,
    quantity_text: str,
) -> None:
    def forbidden_fixed_point_format(*_args, **_kwargs):
        raise AssertionError(
            "Fingerprint storage must not use fixed-point formatting"
        )

    # The original implementation resolves format() through module globals.
    # Make accidental restoration of that implementation fail immediately,
    # before it can allocate an enormous fixed-point string.
    monkeypatch.setattr(
        hourly_module,
        "format",
        forbidden_fixed_point_format,
        raising=False,
    )

    fingerprint = _fingerprint(
        price=Decimal(price_text),
        quantity=Decimal(quantity_text),
    )
    store = hourly_module._TradeIdFingerprintStore()

    try:
        assert store.observe("extreme-exponents", fingerprint) is False
        assert store.observe("extreme-exponents", fingerprint) is True

        connection = store._connection
        assert connection is not None

        row = connection.execute(
            """
            SELECT price, quantity
            FROM trade_fingerprints
            WHERE trade_id = ?
            """,
            ("extreme-exponents",),
        ).fetchone()

        assert row is not None
        assert len(row[0]) < 64
        assert len(row[1]) < 64
        assert Decimal(row[0]) == fingerprint[1]
        assert Decimal(row[1]) == fingerprint[2]
    finally:
        store.close()


def test_fingerprint_storage_accepts_equivalent_decimal_spellings() -> None:
    first = _fingerprint(
        price=Decimal("100.000"),
        quantity=Decimal("0.01000"),
    )
    equivalent = _fingerprint(
        price=Decimal("1E+2"),
        quantity=Decimal("1E-2"),
    )
    store = hourly_module._TradeIdFingerprintStore()

    try:
        assert store.observe("equivalent-spellings", first) is False
        assert store.observe("equivalent-spellings", equivalent) is True
    finally:
        store.close()


@pytest.mark.parametrize("changed_field", ("price", "quantity"))
def test_fingerprint_storage_still_rejects_conflicting_decimal_values(
    changed_field: str,
) -> None:
    first = _fingerprint(
        price=Decimal("100"),
        quantity=Decimal("2"),
    )
    conflicting = _fingerprint(
        price=(
            Decimal("101")
            if changed_field == "price"
            else Decimal("100")
        ),
        quantity=(
            Decimal("3")
            if changed_field == "quantity"
            else Decimal("2")
        ),
    )
    store = hourly_module._TradeIdFingerprintStore()

    try:
        assert store.observe("conflicting-values", first) is False

        with pytest.raises(
            hourly_module.TradeOHLCError,
            match="conflicting normalized content",
        ):
            store.observe("conflicting-values", conflicting)
    finally:
        store.close()


def test_fingerprint_storage_preserves_precision_under_small_context() -> None:
    price = Decimal("12345678901234567890.12345678901234567890")
    quantity = Decimal("0.000000000000000000000000000123456789")
    fingerprint = _fingerprint(price=price, quantity=quantity)
    store = hourly_module._TradeIdFingerprintStore()

    try:
        with localcontext(Context(prec=2)):
            assert store.observe("exact-values", fingerprint) is False
            assert store.observe("exact-values", fingerprint) is True

        connection = store._connection
        assert connection is not None

        row = connection.execute(
            """
            SELECT price, quantity
            FROM trade_fingerprints
            WHERE trade_id = ?
            """,
            ("exact-values",),
        ).fetchone()

        assert row is not None
        assert Decimal(row[0]) == price
        assert Decimal(row[1]) == quantity
    finally:
        store.close()


def test_fingerprint_storage_close_remains_idempotent() -> None:
    store = hourly_module._TradeIdFingerprintStore()
    store.close()
    store.close()

    with pytest.raises(
        hourly_module.TradeOHLCError,
        match="already closed",
    ):
        store.observe(
            "closed-store",
            _fingerprint(
                price=Decimal("100"),
                quantity=Decimal("1"),
            ),
        )