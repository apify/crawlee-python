from typing import Annotated, Literal

from pydantic import BaseModel, Field

from crawlee.browsers._types import BrowserType


SupportedOperatingSystems= Literal["windows", "macos", "linux", "android", "ios"]
SupportedDevices = Literal["desktop", "mobile"]
SupportedHttpVersion = Literal["1", "2"]

class ScreenFingerprint(BaseModel):
    """
    Collection of various attributes from following sources:

    https://developer.mozilla.org/en-US/docs/Web/API/Screen
    https://developer.mozilla.org/en-US/docs/Web/API/ScreenDetailed
    https://developer.mozilla.org/en-US/docs/Web/API/Window
    https://developer.mozilla.org/en-US/docs/Web/API/Element
    """

    avail_height: Annotated[float, Field(alias="availHeight")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Screen/availHeight"""

    avail_width: Annotated[float, Field(alias="availWidth")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Screen/availWidth"""

    avail_top: Annotated[float, Field(alias="availTop")]
    """https://developer.mozilla.org/en-US/docs/Web/API/ScreenDetailed/availTop"""

    avail_left: Annotated[float, Field(alias="availLeft")]
    """https://developer.mozilla.org/en-US/docs/Web/API/ScreenDetailed/left"""

    color_depth: Annotated[float, Field(alias="colorDepth")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Screen/colorDepth"""

    height: float
    """https://developer.mozilla.org/en-US/docs/Web/API/Screen/height"""

    pixel_depth: Annotated[float, Field(alias="pixelDepth")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Screen/pixelDepth"""

    width: float
    """https://developer.mozilla.org/en-US/docs/Web/API/Screen/width"""

    device_pixel_ratio: Annotated[float, Field(alias="devicePixelRatio")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Window/devicePixelRatio"""

    page_x_offset: Annotated[float, Field(alias="pageXOffset")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Window/scrollX"""

    page_y_offset: Annotated[float, Field(alias="pageYOffset")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Window/scrollY"""

    inner_height: Annotated[float, Field(alias="innerHeight")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Window/innerHeight"""

    outer_height: Annotated[float, Field(alias="outerHeight")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Window/outerHeight"""

    outer_width: Annotated[float, Field(alias="outerWidth")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Window/outerWidth"""

    inner_width: Annotated[float, Field(alias="innerWidth")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Window/innerWidth"""

    screen_x: Annotated[float, Field(alias="screenX")]  # Why screenY not present in JS?
    """https://developer.mozilla.org/en-US/docs/Web/API/MouseEvent/screenX"""

    client_width: Annotated[float, Field(alias="clientWidth")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Element/clientWidth"""

    client_height: Annotated[float, Field(alias="clientHeight")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Element/clientHeight"""

    has_hdr: Annotated[bool, Field(alias="hasHDR")] # What is this? A placeholder?


class NavigatorFingerprint(BaseModel):
    """
    Collection of various attributes from following sources:

    https://developer.mozilla.org/en-US/docs/Web/API/Navigator
    """

    user_agent: Annotated[str, Field(alias="userAgent")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/userAgent"""



    user_agent_data: Annotated[dict[str, str | list[dict[str, str]] | bool], Field(alias="userAgentData")]
    """https://developer.mozilla.org/en-US/docs/Web/API/WorkerNavigator/userAgentData

    In JS this is just userAgentData: Record<string, string>, but it can contain more stuff like

    mobile = False
    'fullVersionList' = [
    {'brand': 'Google Chrome', 'version': '131.0.6778.86'},
    {'brand': 'Chromium', 'version': '131.0.6778.86'},
    {'brand': 'Not_A Brand', 'version': '24.0.0.0'}
    ]
    so more generic type should be allowed

    """

    do_not_track: Annotated[str, Field(alias="doNotTrack")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/doNotTrack"""

    app_code_name: Annotated[str, Field(alias="appCodeName")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/appCodeName"""

    app_name: Annotated[str, Field(alias="appName")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/appName"""

    app_version: Annotated[str, Field(alias="appVersion")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/appVersion"""

    oscpu: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/oscpu"""

    webdriver: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/webdriver"""

    language: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/language"""

    languages: list[str]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/languages"""

    platform: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/platform"""

    device_memory: Annotated[float|None, Field(alias="deviceMemory")] = None # Firefox does not have deviceMemory available
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/deviceMemory"""

    hardware_concurrency: Annotated[float, Field(alias="hardwareConcurrency")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/hardwareConcurrency"""

    product: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/product"""

    product_sub: Annotated[str, Field(alias="productSub")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/productSub"""

    vendor: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/vendor"""

    vendor_sub: Annotated[str, Field(alias="vendorSub")]
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/vendorSub"""

    max_touch_points: Annotated[float|None, Field(alias="maxTouchPoints")] = None
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/maxTouchPoints"""

    extra_properties: Annotated[dict[str, str], Field(alias="extraProperties")] = []


class VideoCard(BaseModel):
    rendered: str
    vendor: str

class Fingerprint(BaseModel):
    """Represents specific browser fingerprint collection.

    Collection of browser settings, attributes and capabilities is commonly referred to as a browser fingerprint.
    Such fingerprint can be used to collect various information or track or group users, or thus also detect web
    crawlers by inspecting suspiciously looking browser fingerprints.
    This object contains attributes that are sub set of such specific fingerprint.
    See `https://docs.apify.com/academy/anti-scraping/mitigation/generating-fingerprints` TODO: Update guide with Python example.
    """

    screen: ScreenFingerprint
    navigator: NavigatorFingerprint
    video_codecs: Annotated[dict[str, str], Field(alias="videoCodecs")] = None
    audio_codecs: Annotated[dict[str, str], Field(alias="audioCodecs")] = None
    plugins_data: Annotated[dict[str, str], Field(alias="pluginsData")] = None
    battery: dict[str, str] | None = None
    video_card: VideoCard
    multimedia_devices: Annotated[list[str], Field(alias="multimediaDevices")]
    fonts: list[str] = []
    mock_web_rtc: Annotated[bool, Field(alias="mockWebRTC")]
    slim: Annotated[bool|None, Field(alias="slim")]=None


class ScreenOptions(BaseModel):
    """Defines the screen dimensions of the generated fingerprint."""
    min_width: Annotated[float|None, Field(alias="minWidth")] = None
    max_width: Annotated[float | None, Field(alias="maxWidth")] = None
    min_height: Annotated[float | None, Field(alias="minHeight")] = None
    max_height: Annotated[float | None, Field(alias="maxHeight")] = None

    class Config:
        extra = "forbid"
        populate_by_name = True

class Browser:
    name: BrowserType
    """Name of the browser."""
    min_version: Annotated[float|None, Field(alias="minVersion")] = None
    """Minimum version of browser used."""
    max_version: Annotated[float | None, Field(alias="maxVersion")] = None
    """Maximum version of browser used."""
    http_version: Annotated[SupportedHttpVersion | None, Field(alias="httpVersion")] = None
    """HTTP version to be used for header generation (the headers differ depending on the version)."""

    class Config:
        extra = "forbid"
        populate_by_name = True


class HeaderGeneratorOptions(BaseModel):
    browsers: list[BrowserType] | None = None
    """List of BrowserSpecifications to generate the headers for."""

    operating_systems: Annotated[list[SupportedOperatingSystems]| None, Field(alias="operatingSystems")] = None
    """List of operating systems to generate the headers for."""

    devices: list[SupportedDevices]| None = None
    """List of devices to generate the headers for."""

    locales: list[str]| None = None
    """List of at most 10 languages to include in the [Accept-Language]
    (https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept-Language) request header
    in the language format accepted by that header, for example `en`, `en-US` or `de`."""

    http_version: Annotated[SupportedHttpVersion| None, Field(alias="httpVersion")]= None
    """HTTP version to be used for header generation (the headers differ depending on the version)."""

    strict: bool| None = None
    """If true, the generator will throw an error if it cannot generate headers based on the input."""

    class Config:
        extra = "forbid"
        populate_by_name = True

class FingerprintGeneratorOptions(BaseModel):
    """All generator options are optional. If any value si s not specified, then a default value will be used.

     Default values are implementation detail of used fingerprint generator.
     Specific default values should not be relied upon. Use explicit values if it matters for your use case.
     """

    header_options: HeaderGeneratorOptions | None = None
    screen: ScreenOptions | None = None
    mock_web_rtc: Annotated[bool, Field(alias="mockWebRTC")] = None
    """Whether to mock WebRTC when injecting the fingerprint."""
    slim: Annotated[bool | None, Field(alias="slim")] = None
    """Disables performance-heavy evasions when injecting the fingerprint."""
    class Config:
        extra = "forbid"
        populate_by_name = True
