"""Faker-backed Snowpark UDF generators.

These generators use the Faker library via Snowpark Python UDFs.
They are slower than SQL-native generators but provide richer
fake data types.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from sf_synth.errors import FakerUnavailableError
from sf_synth.generators.base import ColumnGenerator, GeneratorRegistry

if TYPE_CHECKING:
    from snowflake.snowpark import Column, Session


# Suppress noisy Snowpark warnings about local vs server faker version
warnings.filterwarnings(
    "ignore",
    message=r".*The version of package 'faker' in the local environment.*",
)


FAKER_PROVIDER_MAP: dict[str, str] = {
    "email": "email",
    "name": "name",
    "first_name": "first_name",
    "last_name": "last_name",
    "address": "address",
    "street_address": "street_address",
    "city": "city",
    "state": "state",
    "state_abbr": "state_abbr",
    "country": "country",
    "country_code": "country_code",
    "zipcode": "zipcode",
    "postcode": "postcode",
    "phone_number": "phone_number",
    "company": "company",
    "job": "job",
    "text": "text",
    "sentence": "sentence",
    "paragraph": "paragraph",
    "uuid4": "uuid4",
    "url": "url",
    "domain_name": "domain_name",
    "user_name": "user_name",
    "password": "password",
    "ipv4": "ipv4",
    "ipv6": "ipv6",
    "mac_address": "mac_address",
    "credit_card_number": "credit_card_number",
    "ssn": "ssn",
    "date": "date",
    "date_of_birth": "date_of_birth",
    "date_time": "date_time",
    "date_this_year": "date_this_year",
    "date_this_month": "date_this_month",
    "time": "time",
    "currency_code": "currency_code",
    "cryptocurrency_code": "cryptocurrency_code",
    "color_name": "color_name",
    "hex_color": "hex_color",
    "file_name": "file_name",
    "file_extension": "file_extension",
    "mime_type": "mime_type",
}


def _check_faker_availability(session: Session) -> bool:
    """Check if Faker is available in the Snowpark runtime."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            session.add_packages("faker")
        return True
    except Exception:
        return False


def _register_faker_udf(
    session: Session,
    provider: str,
    locale: str = "en_US",
    seed: int | None = None,
) -> str:
    """Register a Faker UDF for the given provider.

    The UDF accepts a row_id (BIGINT) so that each row gets a different
    seed, producing unique values instead of duplicates.

    Args:
        session: Active Snowpark session.
        provider: Faker provider name (e.g., 'email', 'name').
        locale: Faker locale.
        seed: Base random seed for reproducibility.

    Returns:
        The name of the registered UDF.
    """
    from snowflake.snowpark.functions import udf
    from snowflake.snowpark.types import LongType, StringType

    udf_name = f"sf_synth_faker_{provider}_{locale}"

    if seed is not None:
        udf_name = f"{udf_name}_{seed}"

    @udf(
        name=udf_name,
        is_permanent=False,
        replace=True,
        packages=["faker>=18.0.0,<41.0.0"],
        input_types=[LongType()],
        return_type=StringType(),
    )
    def faker_udf(row_id: int) -> str:  # type: ignore[misc]
        from faker import Faker

        row_seed = (seed or 0) + (row_id or 0)
        fake = Faker(locale)
        Faker.seed(row_seed)
        return str(getattr(fake, provider)())

    return udf_name


@GeneratorRegistry.register("faker")
class FakerUDFGenerator(ColumnGenerator):
    """Generate fake data using Faker via Snowpark UDFs."""

    def __init__(
        self,
        column_name: str,
        data_type: str,
        provider: str = "name",
        locale: str = "en_US",
        **kwargs: Any,
    ) -> None:
        super().__init__(column_name, data_type, **kwargs)
        self.provider = provider
        self.locale = locale
        self._udf_name: str | None = None
        self._validate_provider()

    def _validate_provider(self) -> None:
        if self.provider not in FAKER_PROVIDER_MAP:
            valid = ", ".join(sorted(FAKER_PROVIDER_MAP.keys()))
            raise ValueError(
                f"Unknown Faker provider '{self.provider}'. Valid providers: {valid}"
            )

    @property
    def is_sql_native(self) -> bool:
        return False

    def generate(self, session: Session, row_count: int) -> Column:
        from snowflake.snowpark.functions import call_udf

        if not _check_faker_availability(session):
            raise FakerUnavailableError()

        if self._udf_name is None:
            self._udf_name = _register_faker_udf(
                session,
                self.provider,
                self.locale,
                self.seed,
            )

        return call_udf(self._udf_name).alias(self.column_name)


def _register_correlated_faker_udf(
    session: Session,
    group_id: str,
    providers: dict[str, str],
    locale: str = "en_US",
    seed: int | None = None,
) -> str:
    """Register a single Faker UDF that returns a VARIANT object holding
    multiple correlated provider outputs from the *same* Faker instance.

    The combined call ensures attributes like city / state / country come
    from the same Faker locale/profile draw, so they remain semantically
    consistent within one row.

    Args:
        session: Snowpark session.
        group_id: Stable identifier for the correlation group.
        providers: Mapping of output_field_name -> faker_provider_name.
        locale: Faker locale.
        seed: Base seed for reproducibility.

    Returns:
        The registered UDF name. The UDF takes a row_id BIGINT and returns
        a VARIANT (JSON object).
    """
    from snowflake.snowpark.functions import udf
    from snowflake.snowpark.types import LongType, VariantType

    safe_id = group_id.replace("-", "_").replace(".", "_").lower()
    udf_name = f"sf_synth_faker_corr_{safe_id}_{locale}"
    if seed is not None:
        udf_name = f"{udf_name}_{seed}"

    providers_snapshot = dict(providers)

    @udf(
        name=udf_name,
        is_permanent=False,
        replace=True,
        packages=["faker>=18.0.0,<41.0.0"],
        input_types=[LongType()],
        return_type=VariantType(),
    )
    def correlated_faker_udf(row_id: int) -> dict:  # type: ignore[misc]
        from faker import Faker

        row_seed = (seed or 0) + (row_id or 0)
        fake = Faker(locale)
        Faker.seed(row_seed)
        out: dict[str, Any] = {}
        for field_name, prov in providers_snapshot.items():
            try:
                out[field_name] = str(getattr(fake, prov)())
            except Exception:
                out[field_name] = None
        return out

    return udf_name


def _register_regex_udf(
    session: Session,
    seed: int | None = None,
) -> str:
    """Register a regex string generator UDF backed by `exrex`.

    Falls back to a simple character-class implementation when `exrex`
    isn't available in the Snowpark runtime.

    Returns:
        UDF name. Signature: (pattern STRING, row_id BIGINT) -> STRING.
    """
    from snowflake.snowpark.functions import udf
    from snowflake.snowpark.types import LongType, StringType

    udf_name = "sf_synth_regex_generate"
    if seed is not None:
        udf_name = f"{udf_name}_{seed}"

    base_seed = seed or 0

    @udf(
        name=udf_name,
        is_permanent=False,
        replace=True,
        packages=["exrex"],
        input_types=[StringType(), LongType()],
        return_type=StringType(),
    )
    def regex_udf(pattern: str, row_id: int) -> str:  # type: ignore[misc]
        import random as _random

        rng = _random.Random(base_seed + (row_id or 0))
        try:
            import exrex
            try:
                count = exrex.count(pattern)
                if count and count < 100000:
                    idx = rng.randrange(count)
                    val = exrex.getone(pattern, limit=20)
                    return val if val else f"pat_{row_id}"
            except Exception:
                pass
            return exrex.getone(pattern, limit=20) or f"pat_{row_id}"
        except Exception:
            import re as _re
            chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            length = max(1, min(20, len(_re.sub(r"[^A-Za-z0-9]", "", pattern)) or 8))
            return "".join(rng.choice(chars) for _ in range(length))

    return udf_name


class FakerUDFManager:
    """Manager for Faker UDF lifecycle within a session."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self._registered_udfs: set[str] = set()
        self._registered_correlated: dict[str, str] = {}
        self._regex_udf_name: str | None = None
        self._faker_available: bool | None = None

    def check_availability(self) -> bool:
        """Check and cache Faker availability."""
        if self._faker_available is None:
            self._faker_available = _check_faker_availability(self.session)
        return self._faker_available

    def get_or_register_udf(
        self,
        provider: str,
        locale: str = "en_US",
        seed: int | None = None,
    ) -> str:
        """Get existing UDF or register a new one."""
        if not self.check_availability():
            raise FakerUnavailableError()

        udf_key = f"{provider}_{locale}_{seed}"
        if udf_key not in self._registered_udfs:
            udf_name = _register_faker_udf(self.session, provider, locale, seed)
            self._registered_udfs.add(udf_key)
            return udf_name

        return f"sf_synth_faker_{provider}_{locale}" + (f"_{seed}" if seed else "")

    def get_or_register_correlated_udf(
        self,
        group_id: str,
        providers: dict[str, str],
        locale: str = "en_US",
        seed: int | None = None,
    ) -> str:
        """Get or register a correlated faker UDF returning VARIANT.

        Args:
            group_id: Stable identifier for the correlation group (e.g. table.address).
            providers: Mapping of field_name -> faker_provider_name.
            locale: Faker locale.
            seed: Base seed.
        """
        if not self.check_availability():
            raise FakerUnavailableError()

        cache_key = f"{group_id}_{locale}_{seed}"
        if cache_key in self._registered_correlated:
            return self._registered_correlated[cache_key]

        udf_name = _register_correlated_faker_udf(
            self.session, group_id, providers, locale, seed
        )
        self._registered_correlated[cache_key] = udf_name
        return udf_name

    def get_or_register_regex_udf(self, seed: int | None = None) -> str:
        """Register a regex-string generator UDF using `exrex`."""
        if self._regex_udf_name is not None:
            return self._regex_udf_name
        self._regex_udf_name = _register_regex_udf(self.session, seed)
        return self._regex_udf_name

    def cleanup(self) -> None:
        """Drop all registered UDFs."""
        for udf_key in self._registered_udfs:
            parts = udf_key.rsplit("_", 2)
            provider = parts[0]
            locale = parts[1] if len(parts) > 1 else "en_US"
            seed = parts[2] if len(parts) > 2 else None

            udf_name = f"sf_synth_faker_{provider}_{locale}"
            if seed:
                udf_name = f"{udf_name}_{seed}"

            try:
                self.session.sql(f"DROP FUNCTION IF EXISTS {udf_name}(BIGINT)").collect()
            except Exception:
                pass

        for udf_name in self._registered_correlated.values():
            try:
                self.session.sql(f"DROP FUNCTION IF EXISTS {udf_name}(BIGINT)").collect()
            except Exception:
                pass

        if self._regex_udf_name:
            try:
                self.session.sql(
                    f"DROP FUNCTION IF EXISTS {self._regex_udf_name}(STRING, BIGINT)"
                ).collect()
            except Exception:
                pass

        self._registered_udfs.clear()
        self._registered_correlated.clear()
        self._regex_udf_name = None
