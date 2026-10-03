from __future__ import annotations

from decimal import Context, Decimal, localcontext

import pytest

import l2shock.liquidity.block_codec as liquidity_codec
import l2shock.price.block_codec as price_codec

_CANONICALIZERS = (
    (
        liquidity_codec,
        liquidity_codec._canonical_nonnegative_decimal_text,
        liquidity_codec.HourlyBlockLimitError,
    ),
    (
        price_codec,
        price_codec._canonical_positive_decimal_text,
        price_codec.PriceBlockLimitError,
    ),
)


@pytest.mark.parametrize(
    ("module", "canonicalize", "limit_error"),
    _CANONICALIZERS,
    ids=("liquidity", "price"),
)
@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("1", "1"),
        ("1.0000", "1"),
        ("1000.000", "1000"),
        ("123.4500", "123.45"),
        ("0.0300", "0.03"),
        ("0.00000100", "0.000001"),
        ("1E+3", "1000"),
        ("1.2300E+3", "1230"),
        ("1.2300E-3", "0.00123"),
        ("100.00100", "100.001"),
    ),
)
def test_decimal_canonicalization_preserves_existing_bytes(
    module,
    canonicalize,
    limit_error,
    source: str,
    expected: str,
) -> None:
    del module, limit_error

    with localcontext(Context(prec=2)):
        assert (
            canonicalize(
                Decimal(source),
                field_name="test",
            )
            == expected
        )


@pytest.mark.parametrize(
    ("module", "canonicalize", "limit_error"),
    _CANONICALIZERS,
    ids=("liquidity", "price"),
)
@pytest.mark.parametrize("source", ("1E+10000", "1E-10000"))
def test_oversized_decimal_is_rejected_without_fixed_point_formatting(
    monkeypatch: pytest.MonkeyPatch,
    module,
    canonicalize,
    limit_error,
    source: str,
) -> None:
    def forbidden_format(*_args, **_kwargs):
        raise AssertionError("Fixed-point formatting ran before rejection")

    monkeypatch.setattr(
        module,
        "format",
        forbidden_format,
        raising=False,
    )

    with pytest.raises(
        limit_error,
        match="maximum encoded decimal length",
    ):
        canonicalize(Decimal(source), field_name="test")


@pytest.mark.parametrize(
    ("module", "canonicalize", "limit_error"),
    _CANONICALIZERS,
    ids=("liquidity", "price"),
)
def test_decimal_text_length_boundary(
    module,
    canonicalize,
    limit_error,
) -> None:
    del module

    assert (
        len(
            canonicalize(
                Decimal("1E+255"),
                field_name="test",
            )
        )
        == 256
    )
    assert (
        len(
            canonicalize(
                Decimal("1E-254"),
                field_name="test",
            )
        )
        == 256
    )

    for source in ("1E+256", "1E-255"):
        with pytest.raises(limit_error):
            canonicalize(Decimal(source), field_name="test")


@pytest.mark.parametrize(
    ("module", "canonicalize", "limit_error"),
    _CANONICALIZERS,
    ids=("liquidity", "price"),
)
def test_insignificant_scale_does_not_expand_output(
    monkeypatch: pytest.MonkeyPatch,
    module,
    canonicalize,
    limit_error,
) -> None:
    del limit_error

    def forbidden_format(*_args, **_kwargs):
        raise AssertionError("Insignificant scale was expanded")

    monkeypatch.setattr(
        module,
        "format",
        forbidden_format,
        raising=False,
    )

    value = Decimal((0, (1,) + (0,) * 1000, -1000))

    assert canonicalize(value, field_name="test") == "1"


@pytest.mark.parametrize("source", ("0", "-0", "0E-10000", "-0E+10000"))
def test_liquidity_zero_is_canonical_without_exponent_expansion(
    source: str,
) -> None:
    assert (
        liquidity_codec._canonical_nonnegative_decimal_text(
            Decimal(source),
            field_name="test",
        )
        == "0"
    )


@pytest.mark.parametrize("source", ("0", "-0", "NaN", "Infinity", "-1"))
def test_price_rejects_nonpositive_or_nonfinite_values(
    source: str,
) -> None:
    with pytest.raises(price_codec.PriceBlockCodecError):
        price_codec._canonical_positive_decimal_text(
            Decimal(source),
            field_name="test",
        )


@pytest.mark.parametrize("source", ("NaN", "Infinity", "-1"))
def test_liquidity_rejects_negative_or_nonfinite_values(
    source: str,
) -> None:
    with pytest.raises(liquidity_codec.HourlyBlockCodecError):
        liquidity_codec._canonical_nonnegative_decimal_text(
            Decimal(source),
            field_name="test",
        )
