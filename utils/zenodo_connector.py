import os
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class ZenodoConnector:
    """A class to handle Zenodo API interactions for file uploads and metadata management."""
    BASE_URL = "https://zenodo.org/api/deposit"

    def __init__(self, filename, token=None):
        self.filename = filename
        self.token = token or os.getenv('ZENODO_TOKEN')
        if not self.token:
            raise ValueError("ZENODO_TOKEN environment variable not set")
        self.dep_id = None
        self.doi = None

    def create_deposition(self) -> None:
        url = f"{self.BASE_URL}/depositions"
        resp = requests.post(url, params={"access_token": self.token}, json={})
        resp.raise_for_status()
        data = resp.json()
        self.dep_id = data["id"]
        logger.info("Created deposition %s", self.dep_id)

    def upload_file(self) -> None:
        if not self.dep_id:
            raise ValueError(
                "Deposition ID is not set. Please create a deposition first.")
        url = f"{self.BASE_URL}/depositions/{self.dep_id}/files"
        with open(self.filename, "rb") as fp:
            files = {"file": (os.path.basename(self.filename), fp)}
            resp = requests.post(
                url, params={"access_token": self.token}, files=files)
        resp.raise_for_status()
        logger.info("Uploaded %s to deposition %s", self.filename, self.dep_id)

    def add_metadata(self) -> None:
        if not self.dep_id:
            raise ValueError(
                "Deposition ID is not set. Please create a deposition first.")
        current_date = datetime.now(timezone.utc).date().isoformat()
        meta = {
            "metadata": {
                "title":            f"FIShBOT Archive {current_date}",
                "upload_type":      "dataset",
                "description":      "This dataset is a static snapshot of the FIShBOT data product before new data are aggregated. This version of fishbot is archived for reproducibility and is not updated. The data are aggregated from the FIShBOT system, which collects and processes oceanographic from fishery dependent and independent sources.",
                "creators": [
                    {
                        "name":        "Stoltz, Linus",
                        "affiliation": "Commercial Fisheries Research Foundation",
                        "orcid":       "0000-0002-3780-1739"
                    },
                    {
                        "name":        "Maynard, George",
                        "affiliation": "National Oceanic and Atmospheric Administration",
                        "orcid":       "0000-0003-4246-2345"
                    },
                    {
                        "name":        "Morin, Michael",
                        "affiliation": "National Oceanic and Atmospheric Administration",
                        "orcid":       "0009-0000-8901-0827"
                    },
                    {
                        "name":        "Salois, Sarah",
                        "affiliation": "Independent Researcher",
                        "orcid":       "0000-0002-0756-0778"
                    }
                ],
                "access_rights":    "open",
                "license":          "CC-BY-NC-4.0",
                "related_identifiers": [
                    {
                        "relation":   "isSupplementTo",
                        "identifier": "https://github.com/CommercialFisheriesResearchFoundation/fishbot",
                        "scheme":     "url"
                    }
                ],
                "communities": [
                    {
                        "identifier": "fishbot"
                    }
                ]
            }
        }
        url = f"{self.BASE_URL}/depositions/{self.dep_id}"
        resp = requests.put(
            url, params={"access_token": self.token}, json=meta)

        # --- DEBUGGING: print Zenodo’s error message if any ---
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # Zenodo usually returns a JSON body with an "errors" field
            try:
                errors = resp.json().get("errors")
                logger.error("Zenodo rejected metadata: %s", errors)
            except ValueError:
                logger.error(
                    "Zenodo rejected metadata, response text: %s", resp.text)
            raise

        logger.info("Metadata updated for deposition %s", self.dep_id)

    def publish(self) -> None:
        if not self.dep_id:
            raise ValueError(
                "Deposition ID is not set. Please create a deposition first.")
        url = f"{self.BASE_URL}/depositions/{self.dep_id}/actions/publish"
        resp = requests.post(url, params={"access_token": self.token})
        resp.raise_for_status()
        self.doi = resp.json().get("doi")
        logger.info("Published deposition %s → DOI %s", self.dep_id, self.doi)

    def get_doi(self) -> str:
        if not self.doi:
            raise ValueError(
                "DOI is not set. Please publish the deposition first.")
        return self.doi

    def get_dep_id(self) -> int:
        if not self.dep_id:
            raise ValueError(
                "Deposition ID is not set. Please create a deposition first.")
        return self.dep_id

    def get_zenodo_url(self) -> str:
        if not self.dep_id:
            raise ValueError(
                "Deposition ID is not set. Please create a deposition first.")
        return f"{self.BASE_URL}/depositions/{self.dep_id}"
