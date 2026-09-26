from django.conf import settings
from django.core.management import call_command

TEMPLATE = settings.BASE_DIR / "config" / "app_template"

EXPECTED_FILES = [
    "__init__.py",
    "apps.py",
    "models.py",
    "serializers.py",
    "views.py",
    "urls.py",
    "services.py",
    "tasks.py",
    "admin.py",
    "migrations/__init__.py",
    "tests/__init__.py",
]


def test_startapp_template_creates_standard_structure(tmp_path):
    target = tmp_path / "wallets"
    target.mkdir()

    call_command("startapp", "wallets", str(target), template=str(TEMPLATE))

    for relative in EXPECTED_FILES:
        assert (target / relative).is_file(), f"{relative} manquant"

    apps_py = (target / "apps.py").read_text()
    assert "class WalletsConfig(AppConfig):" in apps_py
    assert 'name = "apps.wallets"' in apps_py

    urls_py = (target / "urls.py").read_text()
    assert 'app_name = "wallets"' in urls_py
