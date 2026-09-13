from src.utils import *
from src.services import *
from pathlib import Path

def main():
    sim_config_path = r"config\sim_config.yaml"
    artifacts_config_path = r"config\artifacts.yaml"

    simbox = SimConfigManager(sim_config_path)
    artifactsbox = ArtifactConfigManager(artifacts_config_path).sim_paths

    rawpath = Path(artifactsbox.raw_sim)
    valpath = Path(artifactsbox.validated_sim)
    downpath = Path(artifactsbox.downsampled_sim)

    buckets = int(simbox.downsampling.buckets)

    s = SimulationIngestor(rawpath, valpath)
    s.ingest_and_save()

    d = SimulationDownsampler(buckets, valpath, downpath)
    d.downsample_m4()
    return

if __name__ == "__main__":
    main()
