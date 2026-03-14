"""
PDF to images conversion utilities.
Converts PDF pages to images for better LLM visual analysis.
"""

import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Try to import pdf2image, which requires poppler
try:
    from pdf2image import convert_from_path
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False
    logger.warning("pdf2image not installed. Run: pip install pdf2image")

# Try to import PyMuPDF as fallback
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False


def convert_pdf_to_images(pdf_path, output_dir=None, dpi=150, max_pages=None):
    """
    Convert a PDF to a list of images (one per page).
    
    Args:
        pdf_path: Path to the PDF file
        output_dir: Optional directory to save images (if None, returns PIL images)
        dpi: Resolution for rendering (default 150 for good quality/size balance)
        max_pages: Maximum number of pages to convert (None for all)
    
    Returns:
        List of image paths if output_dir provided, else list of PIL Images
    """
    if not os.path.exists(pdf_path):
        logger.error(f"PDF not found: {pdf_path}")
        return []
    
    # Try PyMuPDF first (faster, no external dependencies)
    if PYMUPDF_AVAILABLE:
        return _convert_with_pymupdf(pdf_path, output_dir, dpi, max_pages)
    
    # Fall back to pdf2image (requires poppler)
    if PDF2IMAGE_AVAILABLE:
        return _convert_with_pdf2image(pdf_path, output_dir, dpi, max_pages)
    
    logger.error("No PDF conversion library available. Install PyMuPDF: pip install PyMuPDF")
    return []


def _convert_with_pymupdf(pdf_path, output_dir, dpi, max_pages):
    """Convert PDF to images using PyMuPDF (fitz)."""
    images = []
    
    try:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        pages_to_convert = min(total_pages, max_pages) if max_pages else total_pages
        
        logger.debug(f"Converting {pages_to_convert}/{total_pages} pages from {os.path.basename(pdf_path)}")
        
        # Calculate zoom factor from DPI (72 is PDF default)
        zoom = dpi / 72
        matrix = fitz.Matrix(zoom, zoom)
        
        for page_num in range(pages_to_convert):
            page = doc[page_num]
            pix = page.get_pixmap(matrix=matrix)
            
            if output_dir:
                # Save to file
                os.makedirs(output_dir, exist_ok=True)
                img_path = os.path.join(output_dir, f"page_{page_num + 1}.png")
                pix.save(img_path)
                images.append(img_path)
            else:
                # Convert to PIL Image
                from PIL import Image
                import io
                img_data = pix.tobytes("png")
                img = Image.open(io.BytesIO(img_data))
                images.append(img)
        
        doc.close()
        logger.debug(f"Converted {len(images)} pages to images")
        return images
        
    except Exception as e:
        logger.error(f"PyMuPDF conversion failed: {str(e)[:100]}")
        return []


def _convert_with_pdf2image(pdf_path, output_dir, dpi, max_pages):
    """Convert PDF to images using pdf2image (requires poppler)."""
    try:
        kwargs = {'dpi': dpi}
        if max_pages:
            kwargs['last_page'] = max_pages
        
        images = convert_from_path(pdf_path, **kwargs)
        
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            saved_paths = []
            for i, img in enumerate(images):
                img_path = os.path.join(output_dir, f"page_{i + 1}.png")
                img.save(img_path, 'PNG')
                saved_paths.append(img_path)
            logger.debug(f"Converted {len(saved_paths)} pages to images")
            return saved_paths
        
        logger.debug(f"Converted {len(images)} pages to images")
        return images
        
    except Exception as e:
        logger.error(f"pdf2image conversion failed: {str(e)[:100]}")
        return []


def get_pdf_page_count(pdf_path):
    """Get the number of pages in a PDF."""
    if PYMUPDF_AVAILABLE:
        try:
            doc = fitz.open(pdf_path)
            count = len(doc)
            doc.close()
            return count
        except:
            pass
    
    if PDF2IMAGE_AVAILABLE:
        try:
            from pdf2image import pdfinfo_from_path
            info = pdfinfo_from_path(pdf_path)
            return info.get('Pages', 0)
        except:
            pass
    
    return 0
