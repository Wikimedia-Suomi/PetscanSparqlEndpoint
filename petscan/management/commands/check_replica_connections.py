import os
from time import perf_counter

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from petscan import enrichment_sql

_REPLICA_SITES = ("fiwiki", "wikidatawiki", "commonswiki")


class Command(BaseCommand):
    help = (
        "Check Toolforge replica connectivity for fiwiki_p, "
        "wikidatawiki_p and commonswiki_p."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--timeout",
            type=int,
            default=int(getattr(settings, "PETSCAN_TIMEOUT_SECONDS", 30)),
            help="Connection timeout in seconds (default: PETSCAN_TIMEOUT_SECONDS or 30).",
        )

    def handle(self, *args, **options):
        if enrichment_sql.pymysql is None:
            raise CommandError("PyMySQL is required for replica connectivity checks.")

        replica_cnf = str(getattr(settings, "TOOLFORGE_REPLICA_CNF", "") or "").strip()
        if not replica_cnf:
            raise CommandError("TOOLFORGE_REPLICA_CNF is required.")
        cnf_path = os.path.expanduser(os.path.expandvars(replica_cnf))

        timeout = int(options["timeout"])
        failed_sites = []

        for site in _REPLICA_SITES:
            host = enrichment_sql._replica_host_for_site(site)
            if host is None:
                failed_sites.append(site)
                self.stderr.write(
                    self.style.ERROR(
                        "[FAIL] site={} db={}_p reason=invalid replica host".format(site, site)
                    )
                )
                continue

            db_name = "{}_p".format(site)
            started_at = perf_counter()
            connection = None

            try:
                connection = enrichment_sql.pymysql.connect(
                    host=host,
                    database=db_name,
                    charset="utf8mb4",
                    connect_timeout=timeout,
                    read_timeout=timeout,
                    write_timeout=timeout,
                    autocommit=True,
                    read_default_file=cnf_path,
                )
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            except Exception as exc:
                elapsed_ms = (perf_counter() - started_at) * 1000.0
                failed_sites.append(site)
                self.stderr.write(
                    self.style.ERROR(
                        "[FAIL] site={} host={} db={} elapsed_ms={:.1f} error={}".format(
                            site,
                            host,
                            db_name,
                            elapsed_ms,
                            exc,
                        )
                    )
                )
            else:
                elapsed_ms = (perf_counter() - started_at) * 1000.0
                self.stdout.write(
                    self.style.SUCCESS(
                        "[OK] site={} host={} db={} elapsed_ms={:.1f}".format(
                            site,
                            host,
                            db_name,
                            elapsed_ms,
                        )
                    )
                )
            finally:
                if connection is not None:
                    try:
                        connection.close()
                    except Exception as exc:
                        self.stderr.write(
                            self.style.WARNING("[WARN] site={} close_error={}".format(site, exc))
                        )

        if failed_sites:
            raise CommandError(
                "Replica connectivity check failed for: {}".format(", ".join(sorted(failed_sites)))
            )

        self.stdout.write(self.style.SUCCESS("Replica connectivity check passed for all sites."))
