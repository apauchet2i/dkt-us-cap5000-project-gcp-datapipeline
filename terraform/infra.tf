terraform {
  backend "gcs" {
    bucket = "dkt-us-data-lake-a1xq-tfstate"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = "dkt-us-data-lake-a1xq"
  region = "us-central1"
}

resource "google_storage_bucket" "dkt-us-cap5000-project-deploy" {
  name = "dkt-us-cap5000-project-deploy"
  storage_class = "REGIONAL"
  location  = "us-central1"
}
