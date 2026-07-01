import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

os.environ.setdefault("AWS_ACCESS_KEY_ID", "origin-access")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "origin-secret")
os.environ.setdefault("OVERLAY_AWS_ACCESS_KEY_ID", "overlay-access")
os.environ.setdefault("OVERLAY_AWS_SECRET_ACCESS_KEY", "overlay-secret")
os.environ.setdefault("START_TIME", "2024-01-10T00:00:00Z")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import main


def local_name(tag):
    return tag.rsplit("}", 1)[-1]


def test_complete_multipart_upload_checksum_elements_are_removed():
    body = b"""
    <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Part>
        <PartNumber>1</PartNumber>
        <ETag>"abc"</ETag>
        <ChecksumCRC32C>AAAAAA==</ChecksumCRC32C>
        <ChecksumSHA256>BBBBBB==</ChecksumSHA256>
      </Part>
      <Part>
        <PartNumber>2</PartNumber>
        <ETag>"def"</ETag>
        <ChecksumCRC64NVME>CCCCCCCCCCC=</ChecksumCRC64NVME>
      </Part>
    </CompleteMultipartUpload>
    """

    rewritten, changed = main.strip_complete_multipart_upload_checksums(body)

    assert changed is True
    root = ET.fromstring(rewritten)
    part_numbers = [elem.text for elem in root.iter() if local_name(elem.tag) == "PartNumber"]
    etags = [elem.text for elem in root.iter() if local_name(elem.tag) == "ETag"]
    checksum_elements = [
        elem for elem in root.iter()
        if local_name(elem.tag) in main.COMPLETE_MULTIPART_CHECKSUM_ELEMENTS
    ]

    assert part_numbers == ["1", "2"]
    assert etags == ['"abc"', '"def"']
    assert checksum_elements == []


def test_non_complete_multipart_upload_xml_is_unchanged():
    body = b"<Tagging><TagSet /></Tagging>"

    rewritten, changed = main.strip_complete_multipart_upload_checksums(body)

    assert changed is False
    assert rewritten == body
