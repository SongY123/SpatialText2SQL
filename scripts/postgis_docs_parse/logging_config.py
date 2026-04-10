import logging

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    tqdm = None


class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            if tqdm is not None:
                tqdm.write(msg)
            else:
                print(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def _configure_logger(name: str, use_tqdm: bool) -> None:
    log = logging.getLogger(name)
    log.handlers.clear()
    log.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    if use_tqdm and tqdm is not None:
        handler: logging.Handler = TqdmLoggingHandler()
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    log.addHandler(handler)
    log.propagate = False


def init_spatial_logging(use_tqdm: bool = False) -> None:
    """Configure the spatial_importer logger (e.g. tqdm-safe when use_tqdm=True)."""
    _configure_logger("spatial_importer", use_tqdm)


def init_pbf_logging(use_tqdm: bool = False) -> None:
    """Configure the osm_pbf_importer logger."""
    _configure_logger("osm_pbf_importer", use_tqdm)


init_spatial_logging(use_tqdm=False)
init_pbf_logging(use_tqdm=False)

logger = logging.getLogger("spatial_importer")
pbf_logger = logging.getLogger("osm_pbf_importer")
