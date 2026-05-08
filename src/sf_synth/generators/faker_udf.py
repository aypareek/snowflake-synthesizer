"""Faker-backed Snowpark UDF generators.

These generators use the Faker library via Snowpark Python UDFs.
They are slower than SQL-native generators but provide richer
fake data types.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sf_synth.errors import FakerUnavailableError
from sf_synth.generators.base import ColumnGenerator, GeneratorRegistry

if TYPE_CHECKING:
    from snowflake.snowpark import Column, Session


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

    Args:
        session: Active Snowpark session.
        provider: Faker provider name (e.g., 'email', 'name').
        locale: Faker locale.
        seed: Random seed for reproducibility.

    Returns:
        The name of the registered UDF.
    """
    from snowflake.snowpark.functions import udf
    from snowflake.snowpark.types import StringType

    udf_name = f"sf_synth_faker_{provider}_{locale}"

    if seed is not None:
        udf_name = f"{udf_name}_{seed}"

    @udf(
        name=udf_name,
        is_permanent=False,
        replace=True,
        packages=["faker"],
        return_type=StringType(),
    )
    def faker_udf() -> str:  # type: ignore[misc]
        from faker import Faker

        fake = Faker(locale)
        if seed is not None:
            Faker.seed(seed)
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


class FakerUDFManager:
    """Manager for Faker UDF lifecycle within a session."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self._registered_udfs: set[str] = set()
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
                self.session.sql(f"DROP FUNCTION IF EXISTS {udf_name}()").collect()
            except Exception:
                pass

        self._registered_udfs.clear()
