from typing import Annotated

from pydantic import BaseModel, Field

class ScreenFingerprint(BaseModel):
    """
    Collection of various attributes from following sources:

    https://developer.mozilla.org/en-US/docs/Web/API/Screen
    https://developer.mozilla.org/en-US/docs/Web/API/ScreenDetailed
    https://developer.mozilla.org/en-US/docs/Web/API/Window
    https://developer.mozilla.org/en-US/docs/Web/API/Element/clientWidth
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

    user_agent_data: Annotated[str, Field(alias="userAgentData")]
    """https://developer.mozilla.org/en-US/docs/Web/API/WorkerNavigator/userAgentData"""

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

    productSub: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/productSub"""

    vendor: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/vendor"""

    vendorSub: str
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/vendorSub"""

    max_touch_points: Annotated[float|None, Field(alias="maxTouchPoints")] = None
    """https://developer.mozilla.org/en-US/docs/Web/API/Navigator/maxTouchPoints"""

    extraProperties: dict[str, str]


class Fingerprint(BaseModel):
    """Represents specific browser fingerprint collection.

    Collection of browser settings, attributes and capabilities is commonly referred to as a browser fingerprint.
    Such fingerprint can be used to collect various information or track or group users, or thus also detect web
    crawlers by inspecting suspiciously looking browser fingerprints.
    This object contains attributes that are sub set of such specific fingerprint.
    See `https://docs.apify.com/academy/anti-scraping/mitigation/generating-fingerprints`
    TODO: Update guide with Python example."""

    screen: ScreenFingerprint
    navigator: NavigatorFingerprint
