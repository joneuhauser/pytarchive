import re
from textwrap import dedent
import pytest
from pyfakefs.fake_filesystem_unittest import Patcher
from pyarchive.service.db import JsonDatabase


@pytest.fixture
def setup_db():
    with Patcher() as patcher:
        patcher.fs.create_file("/var/lib/pyarchive/database.json", contents="[]")
        patcher.fs.create_file(
            "/etc/pyarchive.conf",
            contents="[Device]\nlibrary = /dev/sch0 \ndrive_serial=10WT017752\n[General]\ntape_max_size=17000000000",
        )
        db = JsonDatabase()
        yield db


def test_create_entry(setup_db):
    db = setup_db
    entry = db.create_entry("/nas/test_project", "Test project description")
    assert entry["original_directory"] == "/nas/test_project"
    assert entry["state"] == "preparing"
    assert entry["description"] == "Test project description"


def test_create_entry_duplicate(setup_db):
    db = setup_db
    db.create_entry("/nas/test_project", "Test project description")
    with pytest.raises(ValueError):
        db.create_entry("/nas/test_project", "Test project description 2")


def test_state_change(setup_db):
    db = setup_db
    entry = db.create_entry("/nas/test_project", "Test project description")
    modified_entry = db.set_prepared(entry, 123456)
    assert modified_entry["state"] == "prepared"
    assert modified_entry["size"] == 123456
    assert "size_queried" in modified_entry
    modified_entry = db.set_archiving_queued(entry, "AAK123")
    assert modified_entry["state"] == "archiving_queued"
    assert modified_entry["tape"] == "AAK123"
    modified_entry = db.set_archiving(entry, "test_project_path")
    assert modified_entry["state"] == "archiving"
    assert modified_entry["path_on_tape"] == "test_project_path"
    modified_entry = db.set_archived(entry)
    assert modified_entry["state"] == "archived"
    assert "archived" in modified_entry


def test_get_entries_by_state(setup_db):
    db = setup_db
    db.create_entry("/nas/project_1", "Project 1 description")
    entry = db.create_entry("/nas/project_2", "Project 2 description")
    db.set_prepared(entry, 123456)
    entries = db.get_entries_by_state("prepared")
    assert len(entries) == 1
    assert entries[0]["original_directory"] == "/nas/project_2"


def test_get_directories_on_tape(setup_db):
    db = setup_db
    db.create_entry("/nas/project_1", "Project 1 description")
    entry = db.create_entry("/nas/project_2", "Project 2 description")
    entry = db.set_prepared(entry, 123456)
    entry = db.set_archiving_queued(entry, "AAK123")
    directories = db.get_directories_on_tape("AAK123")
    assert len(directories) == 1
    assert directories[0]["original_directory"] == "/nas/project_2"


def test_print(setup_db):
    db = setup_db
    db.create_entry("/path/to/folder", "Description1")

    entry = db.create_entry("/folder1", "Description folder 1")
    entry = db.set_prepared(entry, 9e9)
    entry = db.create_entry("/folder2", "Description folder 2")
    entry = db.set_prepared(entry, 7e9)

    entry = db.create_entry("/folder3", "Description folder 3")
    entry = db.set_prepared(entry, 2e7)
    entry = db.set_archiving_queued(entry, "AAK123")
    entry = db.create_entry("/folder4", "Description folder 4")
    entry = db.set_prepared(entry, 5e9)
    entry = db.set_archiving_queued(entry, "AAK123")

    entry = db.create_entry("/folder5", "Description folder 5")
    entry = db.set_prepared(entry, 1.01e6)
    entry = db.set_archiving_queued(entry, "AAK125")
    entry = db.set_archiving(entry, "folder5")

    entry = db.create_entry("/folder6", "Description folder 6")
    entry = db.set_prepared(entry, 2e9)
    entry = db.set_archiving_queued(entry, "AAK123")
    entry = db.set_archiving(entry, "folder6")
    entry = db.set_archived(entry)

    entry = db.create_entry("/folder7", "Description folder 7")
    entry = db.set_prepared(entry, 1e9)
    entry = db.set_archiving_queued(entry, "AAK123")
    entry = db.set_archiving(entry, "folder7")
    entry = db.set_archived(entry)

    entry = db.create_entry("/folder8", "Description folder 8")
    entry = db.set_prepared(entry, 7e9)
    entry = db.set_archiving_queued(entry, "AAK124")
    entry = db.set_archiving(entry, "folder8")
    entry = db.set_archived(entry)

    data = db.format(all_tapes=["AAK123", "AAK124", "AAK125", "AAK126"])
    print(data)
    date_pattern = r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}\b"

    # Replace all occurrences of date-like patterns with '<date>'
    data = re.sub(date_pattern, "<date>", data)

    assert (
        data
        == dedent("""
        [preparing]
        /path/to/folder: Description1

        [prepared]
        /folder1 (8.4 TiB as of <date>) -> (suggested: AAK124)
            Description folder 1
        /folder2 (6.5 TiB as of <date>) -> (suggested: AAK123)
            Description folder 2

        [archiving_queued]
        /folder3 (19.1 GiB as of <date>) -> AAK123
        /folder4 (4.7 TiB as of <date>) -> AAK123

        [archiving]
        /folder5 (986.3 MiB) -> AAK125

        Tape overview:
        AAK123 7.5 TiB / 15.8 TiB (47.18%)
        \033[33m    /folder4 (4.7 TiB) Description folder 4 [archiving_queued]\033[0m
            /folder6 (1.9 TiB) Description folder 6
            /folder7 (953.7 GiB) Description folder 7
        \033[33m    /folder3 (19.1 GiB) Description folder 3 [archiving_queued]\033[0m
        AAK124 6.5 TiB / 15.8 TiB (41.18%)
            /folder8 (6.5 TiB) Description folder 8
        AAK125 986.3 MiB / 15.8 TiB (0.01%)
        \033[33m    /folder5 (986.3 MiB) Description folder 5 [archiving]\033[0m
        AAK126 0 Bytes / 15.8 TiB (0.00%)""")[1:]
    )  # remove leading newline
