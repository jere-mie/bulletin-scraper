from __future__ import annotations

import logging
import os
from io import BytesIO


logger = logging.getLogger(__name__)

try:
    from pdf2image import convert_from_path

    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False

try:
    import fitz

    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

if not PYMUPDF_AVAILABLE and not PDF2IMAGE_AVAILABLE:
    logger.warning("No PDF conversion library available. Install PyMuPDF or pdf2image.")


def convert_pdf_to_images(pdf_path: str, output_dir: str | None = None, dpi: int = 150, max_pages: int | None = None):
    if not os.path.exists(pdf_path):
        logger.error("PDF not found: %s", pdf_path)
        return []
    if PYMUPDF_AVAILABLE:
        return _convert_with_pymupdf(pdf_path, output_dir, dpi, max_pages)
    if PDF2IMAGE_AVAILABLE:
        return _convert_with_pdf2image(pdf_path, output_dir, dpi, max_pages)
    logger.error("No PDF conversion library available. Install PyMuPDF: pip install PyMuPDF")
    return []


def _convert_with_pymupdf(pdf_path: str, output_dir: str | None, dpi: int, max_pages: int | None):
    images = []
    try:
        document = fitz.open(pdf_path)
        total_pages = len(document)
        pages_to_convert = min(total_pages, max_pages) if max_pages else total_pages
        logger.debug("Converting %s/%s pages from %s", pages_to_convert, total_pages, os.path.basename(pdf_path))
        zoom = dpi / 72
        matrix = fitz.Matrix(zoom, zoom)

        for page_number in range(pages_to_convert):
            page = document[page_number]
            pixmap = page.get_pixmap(matrix=matrix)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                image_path = os.path.join(output_dir, f"page_{page_number + 1}.png")
                pixmap.save(image_path)
                images.append(image_path)
            else:
                from PIL import Image

                image = Image.open(BytesIO(pixmap.tobytes("png")))
                images.append(image)

        document.close()
        logger.debug("Converted %s pages to images", len(images))
        return images
    except Exception as exc:
        logger.error("PyMuPDF conversion failed: %s", str(exc)[:100])
        return []


def _convert_with_pdf2image(pdf_path: str, output_dir: str | None, dpi: int, max_pages: int | None):
    try:
        kwargs = {"dpi": dpi}
        if max_pages:
            kwargs["last_page"] = max_pages
        images = convert_from_path(pdf_path, **kwargs)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            saved_paths = []
            for index, image in enumerate(images):
                image_path = os.path.join(output_dir, f"page_{index + 1}.png")
                image.save(image_path, "PNG")
                saved_paths.append(image_path)
            logger.debug("Converted %s pages to images", len(saved_paths))
            return saved_paths
        logger.debug("Converted %s pages to images", len(images))
        return images
    except Exception as exc:
        logger.error("pdf2image conversion failed: %s", str(exc)[:100])
        return []


def get_pdf_page_count(pdf_path: str) -> int:
    if PYMUPDF_AVAILABLE:
        try:
            document = fitz.open(pdf_path)
            count = len(document)
            document.close()
            return count
        except Exception:
            pass

    if PDF2IMAGE_AVAILABLE:
        try:
            from pdf2image import pdfinfo_from_path

            info = pdfinfo_from_path(pdf_path)
            return info.get("Pages", 0)
        except Exception:
            pass

    return 0