from pathlib import Path


# Paths
TEST_ROOT = Path(__file__).parent.resolve()
LIBRARY_ROOT = TEST_ROOT.parent
ASSETS_DIR = TEST_ROOT / "assets"
MODEL_FILES_DIR = ASSETS_DIR / "model_files"
MODEL_LOGOS_DIR = ASSETS_DIR / "model_logos"
SAMPLE_INPUTS_DIR = ASSETS_DIR / "sample_inputs"

# Test Model Files
MODEL_FILE_AUDIO = "audio_qa_model.pt"
MODEL_FILE_IMAGE = "image_qa_model.pt"
MODEL_FILE_TEXT = "text_qa_model.pt"
MODEL_FILE_VIDEO = "video_qa_model.pt"

# Test Logo Files
LOGO_APPLIHUE = "Applihue_logo.jpg"
LOGO_COLORIZE = "Colorize_logo.jpg"
LOGO_DELPHI = "Delphi_logo.jpg"
LOGO_DELPHI_MIRROR = "Delphi_logo_mirror.jpg"
LOGO_ERISED = "Erised_logo.jpg"
LOGO_HEBREW_UNIVERSITY = "hebrew_university_logo.png"
LOGO_LONGSHORT = "Longshort_logo.png"
LOGO_REFLECTIX = "Reflectix_logo.png"
LOGO_SHORTPAGE = "Shortpage_logo.png"
LOGO_SOUNDHOUND = "Soundhound_logo.png"
LOGO_TOONIFY = "Toonify_logo.jpg"
LOGO_VINCITEXT = "Vincitext_logo.jpg"
LOGO_WAVERSE = "Waverse_logo.jpg"
LOGO_YONAVOX = "Yonavox_logo.png"

# Sample Input Files
# Audio
SAMPLE_INPUT_AAC = "sample_input.aac"
SAMPLE_INPUT_MP3 = "sample_input.mp3"
SAMPLE_INPUT_WAV = "sample_input.wav"

# Video
SAMPLE_INPUT_AVI = "sample_input.avi"
SAMPLE_INPUT_MKV = "sample_input.mkv"
SAMPLE_INPUT_MOV = "sample_input.mov"
SAMPLE_INPUT_MP4 = "sample_input.mp4"
SAMPLE_INPUT_WEBM = "sample_input.webm"
SAMPLE_INPUT_WMV = "sample_input.wmv"

# Image
SAMPLE_INPUT_JPG = "sample_input.jpg"
SAMPLE_INPUT_PNG = "sample_input.png"
SAMPLE_INPUT_WEBP = "sample_input.webp"

# Text
SAMPLE_INPUT_TXT = "sample_input.txt"


# Default/Placeholder IDs
DEFAULT_ID = "0"
DEFAULT_MODEL_ID = "0"
DEFAULT_VERSION_ID = "0"
DEFAULT_SIGNATURE_ID = "0"


# Test Account Data
TEST_ACCOUNT_ID = "account-123"
TEST_ACCOUNT_ID_ALT = "account-456"


# Data Encodings
ENCODING_UTF8 = "utf8"
ENCODING_WAV = "wav"
ENCODING_MP3 = "mp3"
ENCODING_MP4 = "mp4"
ENCODING_PNG = "png"
ENCODING_JPG = "jpg"


# Model Metadata (for integration tests)
MODEL_CATEGORY_QA = "QA Model"
MODEL_TAGS_DEFAULT = ["SLM", "QA", "Test"]

VERSION_SHORT_DESC = (
    "An end to end quality assurance model, used to test the model upload process through our SDK."
)
VERSION_LONG_DESC = (
    "This language model can also be used during inference to test the input and output signature "
    "processing of text models. Simply write some text and have it transformed by this genuine AI model."
)
