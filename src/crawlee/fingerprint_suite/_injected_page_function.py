# This is function that should set the fingerprint on page object.

_unclosed_body = r"""
(() =>{
    const isHeadlessChromium = /headless/i.test(navigator.userAgent) && navigator.plugins.length === 0;
    const isChrome = navigator.userAgent.includes("Chrome");
    const isFirefox = navigator.userAgent.includes("Firefox");
    const isSafari = navigator.userAgent.includes("Safari") && !navigator.userAgent.includes("Chrome");

    let slim = null;
    function getSlim() {
        if(slim === null) {
            slim = window.slim || false;
            if(typeof window.slim !== 'undefined') {
                delete window.slim;
            }
        }

        return slim;
    }

    // This file contains utils that are build and included on the window object with some randomized prefix.

    // some protections can mess with these to prevent the overrides - our script is first so we can reference the old values.
    const cache = {
        Reflect: {
            get: Reflect.get.bind(Reflect),
            apply: Reflect.apply.bind(Reflect),
        },
        // Used in `makeNativeString`
        nativeToStringStr: `${Function.toString}`, // => `function toString() { [native code] }`
    };

    /**
     * @param masterObject Object to override.
     * @param propertyName Property to override.
     * @param proxyHandler Proxy handled with the new value.
     */
    function overridePropertyWithProxy(masterObject, propertyName, proxyHandler) {
        const originalObject = masterObject[propertyName];
        const proxy = new Proxy(masterObject[propertyName], stripProxyFromErrors(proxyHandler));

        redefineProperty(masterObject, propertyName, { value: proxy });
        redirectToString(proxy, originalObject);
    }

    const prototypeProxyHandler = {
        setPrototypeOf: (target, newProto) => {
            try {
                throw new TypeError('Cyclic __proto__ value');
            } catch (e) {
                const oldStack = e.stack;
                const oldProto = Object.getPrototypeOf(target);
                Object.setPrototypeOf(target, newProto);
                try {
                    // shouldn't throw if prototype is okay, will throw if there is a prototype cycle (maximum call stack size exceeded).
                    target['nonexistentpropertytest'];
                    return true;
                }
                catch (err) {
                    Object.setPrototypeOf(target, oldProto);
                    if (oldStack.includes('Reflect.setPrototypeOf')) return false;
                    const newError = new TypeError('Cyclic __proto__ value');
                    const stack = oldStack.split('\n');
                    newError.stack = [stack[0], ...stack.slice(2)].join('\n');
                    throw newError;
                }
            }
        },
    }

    function useStrictModeExceptions(prop) {
        if (['caller', 'callee', 'arguments'].includes(prop)) {
            throw TypeError(`'caller', 'callee', and 'arguments' properties may not be accessed on strict mode functions or the arguments objects for calls to them`);
        }
    }

    /**
     * @param masterObject Object to override.
     * @param propertyName Property to override.
     * @param proxyHandler ES6 Proxy handler object with a get handle only.
     */
    function overrideGetterWithProxy(masterObject, propertyName, proxyHandler) {
        const fn = Object.getOwnPropertyDescriptor(masterObject, propertyName).get;
        const fnStr = fn.toString; // special getter function string
        const proxyObj = new Proxy(fn, {
            ...stripProxyFromErrors(proxyHandler),
            ...prototypeProxyHandler,
        });

        redefineProperty(masterObject, propertyName, { get: proxyObj });
        redirectToString(proxyObj, fnStr);
    }

    /**
     * @param instance Instance to override.
     * @param overrideObj New instance values.
     */
    // eslint-disable-next-line no-unused-vars
    function overrideInstancePrototype(instance, overrideObj) {
        try {
            Object.keys(overrideObj).forEach((key) => {
                if (!(overrideObj[key] === null)) {
                    try {
                        overrideGetterWithProxy(
                            Object.getPrototypeOf(instance),
                            key,
                            makeHandler().getterValue(overrideObj[key]),
                        );
                    } catch (e) {
                        return false;
                        // console.error(`Could not override property: ${key} on ${instance}. Reason: ${e.message} `); // some fingerprinting services can be listening
                    }
                }
            });
        } catch (e) {
            console.error(e);
        }
    }

    /**
     * Updates the .toString method in Function.prototype to return a native string representation of the function.
     * @param {*} proxyObj
     * @param {*} originalObj
     */
    function redirectToString(proxyObj, originalObj) {
        if(getSlim()) return;

        const handler = {
            setPrototypeOf: (target, newProto) => {
                try {
                    throw new TypeError('Cyclic __proto__ value');
                } catch (e) {
                    if (e.stack.includes('Reflect.setPrototypeOf')) return false;
                    // const stack = e.stack.split('\n');
                    // e.stack = [stack[0], ...stack.slice(2)].join('\n');
                    throw e;
                }
            },
            apply(target, ctx) {
                // This fixes e.g. `HTMLMediaElement.prototype.canPlayType.toString + ""`
                if (ctx === Function.prototype.toString) {
                    return makeNativeString('toString');
                }

                // `toString` targeted at our proxied Object detected
                if (ctx === proxyObj) {
                    // Return the toString representation of our original object if possible
                    return makeNativeString(proxyObj.name);
                }

                // Check if the toString prototype of the context is the same as the global prototype,
                // if not indicates that we are doing a check across different windows., e.g. the iframeWithdirect` test case
                const hasSameProto = Object.getPrototypeOf(
                    Function.prototype.toString,
                ).isPrototypeOf(ctx.toString); // eslint-disable-line no-prototype-builtins

                if (!hasSameProto) {
                    // Pass the call on to the local Function.prototype.toString instead
                    return ctx.toString();
                }

                if (Object.getPrototypeOf(ctx) === proxyObj){
                    try {
                        return target.call(ctx);
                    } catch (err) {
                        err.stack = err.stack.replace(
                            'at Object.toString (',
                            'at Function.toString (',
                        );
                    throw err;
                }}
                return target.call(ctx);
            },
            get: function(target, prop, receiver) {
                if (prop === 'toString') {
                  return new Proxy(target.toString, {
                    apply: function(tget, thisArg, argumentsList) {
                        try {
                            return tget.bind(thisArg)(...argumentsList);
                        } catch (err) {
                            if(Object.getPrototypeOf(thisArg) === tget){
                                err.stack = err.stack.replace(
                                    'at Object.toString (',
                                    'at Function.toString (',
                                );
                            }

                            throw err;
                        }
                    }
                  });
                }
                useStrictModeExceptions(prop);
                return Reflect.get(...arguments);
              }
        };

        const toStringProxy = new Proxy(
            Function.prototype.toString,
            stripProxyFromErrors(handler),
        );
        redefineProperty(Function.prototype, 'toString', {
            value: toStringProxy,
        });
    }

    function makeNativeString(name = '') {
        return cache.nativeToStringStr.replace('toString', name || '');
    }

    function redefineProperty(masterObject, propertyName, descriptorOverrides = {}) {
        return Object.defineProperty(masterObject, propertyName, {
            // Copy over the existing descriptors (writable, enumerable, configurable, etc)
            ...(Object.getOwnPropertyDescriptor(masterObject, propertyName) || {}),
            // Add our overrides (e.g. value, get())
            ...descriptorOverrides,
        });
    }

    /**
     * For all the traps in the passed proxy handler, we wrap them in a try/catch and modify the error stack if they throw.
     * @param {*} handler A proxy handler object
     * @returns A new proxy handler object with error stack modifications
     */
    function stripProxyFromErrors(handler) {
        const newHandler = {};
        // We wrap each trap in the handler in a try/catch and modify the error stack if they throw
        const traps = Object.getOwnPropertyNames(handler);
        traps.forEach((trap) => {
            newHandler[trap] = function () {
                try {
                    // Forward the call to the defined proxy handler
                    return handler[trap].apply(this, arguments || []); //eslint-disable-line
                } catch (err) {
                    // Stack traces differ per browser, we only support chromium based ones currently
                    if (!err || !err.stack || !err.stack.includes(`at `)) {
                        throw err;
                    }

                    // When something throws within one of our traps the Proxy will show up in error stacks
                    // An earlier implementation of this code would simply strip lines with a blacklist,
                    // but it makes sense to be more surgical here and only remove lines related to our Proxy.
                    // We try to use a known "anchor" line for that and strip it with everything above it.
                    // If the anchor line cannot be found for some reason we fall back to our blacklist approach.

                    const stripWithBlacklist = (stack, stripFirstLine = true) => {
                        const blacklist = [
                            `at Reflect.${trap} `, // e.g. Reflect.get or Reflect.apply
                            `at Object.${trap} `, // e.g. Object.get or Object.apply
                            `at Object.newHandler.<computed> [as ${trap}] `, // caused by this very wrapper :-)
                            `at newHandler.<computed> [as ${trap}] `,        // also caused by this wrapper :p
                        ];
                        return (
                            err.stack
                                .split('\n')
                                // Always remove the first (file) line in the stack (guaranteed to be our proxy)
                                .filter((line, index) => !(index === 1 && stripFirstLine))
                                // Check if the line starts with one of our blacklisted strings
                                .filter((line) => !blacklist.some((bl) => line.trim().startsWith(bl)))
                                .join('\n')
                        );
                    };

                    const stripWithAnchor = (stack, anchor) => {
                        const stackArr = stack.split('\n');
                        anchor = anchor || `at Object.newHandler.<computed> [as ${trap}] `; // Known first Proxy line in chromium
                        const anchorIndex = stackArr.findIndex((line) => line.trim().startsWith(anchor));
                        if (anchorIndex === -1) {
                            return false; // 404, anchor not found
                        }
                        // Strip everything from the top until we reach the anchor line
                        // Note: We're keeping the 1st line (zero index) as it's unrelated (e.g. `TypeError`)
                        stackArr.splice(1, anchorIndex);
                        return stackArr.join('\n');
                    };


                    const oldStackLines = err.stack.split('\n');
                    Error.captureStackTrace(err);
                    const newStackLines = err.stack.split('\n');

                    err.stack = [newStackLines[0],oldStackLines[1],...newStackLines.slice(1)].join('\n');

                    if ((err.stack || '').includes('toString (')) {
                        err.stack = stripWithBlacklist(err.stack, false);
                        throw err;
                    }

                    // Try using the anchor method, fallback to blacklist if necessary
                    err.stack = stripWithAnchor(err.stack) || stripWithBlacklist(err.stack);

                    throw err; // Re-throw our now sanitized error
                }
            };
        });
        return newHandler;
    }

    // eslint-disable-next-line no-unused-vars
    function overrideWebGl(webGl) {
        // try to override WebGl
        try {
            const getParameterProxyHandler = {
                apply: function (target, ctx, args) {
                    const param = (args || [])[0];
                    const result = cache.Reflect.apply(target, ctx, args);
                    // UNMASKED_VENDOR_WEBGL
                    if (param === 37445) {
                        return webGl.vendor;
                    }
                    // UNMASKED_RENDERER_WEBGL
                    if (param === 37446) {
                        return webGl.renderer;
                    }
                    return result;
                },
                get: function (target, prop, receiver) {
                    useStrictModeExceptions(prop);
                    return Reflect.get(...arguments);
                },
            }
            const addProxy = (obj, propName) => {
                overridePropertyWithProxy(obj, propName, getParameterProxyHandler);
            }
            // For whatever weird reason loops don't play nice with Object.defineProperty, here's the next best thing:
            addProxy(WebGLRenderingContext.prototype, 'getParameter');
            addProxy(WebGL2RenderingContext.prototype, 'getParameter');
        } catch (err) {
            console.warn(err);
        }
    }

    // eslint-disable-next-line no-unused-vars
    const overrideCodecs = (audioCodecs, videoCodecs) => {
        try {
            const codecs = {
                ...Object.fromEntries(Object.entries(audioCodecs).map(([key, value]) => [`audio/${key}`, value])),
                ...Object.fromEntries(Object.entries(videoCodecs).map(([key, value]) => [`video/${key}`, value])),
            };

            const findCodec = (codecString) => {
                const [mime, codecSpec] = codecString.split(';');
                if (mime === 'video/mp4') {
                    if (codecSpec && codecSpec.includes('avc1.42E01E')) { // codec is missing from Chromium
                        return {name: mime, state: 'probably'};
                    }
                }

                const codec = Object.entries(codecs).find(([key]) => key === codecString.split(';')[0]);
                if(codec) {
                    return {name: codec[0], state: codec[1]};
                }

                return undefined;
            };

            const canPlayType = {
                // eslint-disable-next-line
                apply: function (target, ctx, args) {
                    if (!args || !args.length) {
                        return target.apply(ctx, args);
                    }
                    const [codecString] = args;
                    const codec = findCodec(codecString);

                    if (codec) {
                        return codec.state;
                    }

                    // If the codec is not in our collected data use
                    return target.apply(ctx, args);
                },
            };

            overridePropertyWithProxy(
                HTMLMediaElement.prototype,
                'canPlayType',
                canPlayType,
            );
        } catch (e) {
            console.warn(e);
        }
    };

    // eslint-disable-next-line no-unused-vars
    function overrideBattery(batteryInfo) {
        try {
            const getBattery = {
                ...prototypeProxyHandler,
                // eslint-disable-next-line
                apply: async function () {
                    return batteryInfo;
                },
            };

            if(navigator.getBattery) { // Firefox does not have this method - to be fixed
                overridePropertyWithProxy(
                    Object.getPrototypeOf(navigator),
                    'getBattery',
                    getBattery,
                );
            }
        } catch (e) {
            console.warn(e);
        }
    }

    function overrideIntlAPI(language){
        try {
            const innerHandler = {
                construct(target, [locales, options]) {
                  return new target(locales ?? language, options);
                },
                apply(target, _, [locales, options]) {
                    return target(locales ?? language, options);
                }
              };

            overridePropertyWithProxy(window, 'Intl', {
                get(target, key){
                    if(typeof key !== 'string' || key[0].toLowerCase() === key[0]) return target[key];
                    return new Proxy(
                        target[key],
                        innerHandler
                    );
                }
            });
        } catch (e) {
            console.warn(e);
        }
    }

    function makeHandler() {
        return {
            // Used by simple `navigator` getter evasions
            getterValue: (value) => ({
                apply(target, ctx, args) {
                    // Let's fetch the value first, to trigger and escalate potential errors
                    // Illegal invocations like `navigator.__proto__.vendor` will throw here
                    const ret = cache.Reflect.apply(...arguments); // eslint-disable-line
                    if (args && args.length === 0) {
                        return value;
                    }
                    return ret;
                },
                get: function (target, prop, receiver) {
                    useStrictModeExceptions(prop);
                    return Reflect.get(...arguments);
                },
            }),
        };
    }

    function overrideScreenByReassigning(target, newProperties) {
        for (const [prop, value] of Object.entries(newProperties)) {
            if (value > 0) {
                // The 0 values are introduced by collecting in the hidden iframe.
                // They are document sizes anyway so no need to test them or inject them.
                target[prop] = value;
            }
        }
    }

    // eslint-disable-next-line no-unused-vars
    function overrideWindowDimensionsProps(props) {
        try {
            overrideScreenByReassigning(window, props);
        } catch (e) {
            console.warn(e);
        }
    }

    // eslint-disable-next-line no-unused-vars
    function overrideDocumentDimensionsProps(props) {
        try {
            // FIX THIS = non-zero values here block the injecting process?
            // overrideScreenByReassigning(window.document.body, props);
        } catch (e) {
            console.warn(e);
        }
    }

    function replace(target, key, value) {
        if (target?.[key]) {
            target[key] = value;
        }
    }

    // Replaces all the WebRTC related methods with a recursive ES6 Proxy
    // This way, we don't have to model a mock WebRTC API and we still don't get any exceptions.
    function blockWebRTC() {
        const handler = {
            get: () => {
                return new Proxy(() => {}, handler);
            },
            apply: () => {
                return new Proxy(() => {}, handler);
            },
            construct: () => {
                return new Proxy(() => {}, handler);
            },
        };

        const ConstrProxy = new Proxy(Object, handler);
        const proxy = new Proxy(() => {}, handler);

        replace(navigator.mediaDevices, 'getUserMedia', proxy);
        replace(navigator, 'webkitGetUserMedia', proxy);
        replace(navigator, 'mozGetUserMedia', proxy);
        replace(navigator, 'getUserMedia`', proxy);
        replace(window, 'webkitRTCPeerConnection', proxy);

        replace(window, 'RTCPeerConnection', ConstrProxy);
        replace(window, 'MediaStreamTrack', ConstrProxy);
    }

    // eslint-disable-next-line no-unused-vars
    function overrideUserAgentData(userAgentData) {
        try {
            const { brands, mobile, platform, ...highEntropyValues } = userAgentData;
            // Override basic properties
            const getHighEntropyValues = {
                // eslint-disable-next-line
                apply: async function (target, ctx, args) {
                    // Just to throw original validation error
                    // Remove traces of our Proxy
                    const stripErrorStack = (stack) => stack
                        .split('\n')
                        .filter((line) => !line.includes('at Object.apply'))
                        .filter((line) => !line.includes('at Object.get'))
                        .join('\n');

                    try {
                        if (!args || !args.length) {
                            return target.apply(ctx, args);
                        }
                        const [hints] = args;
                        await target.apply(ctx, args);

                        const data = { brands, mobile, platform };
                        hints.forEach((hint) => {
                            data[hint] = highEntropyValues[hint];
                        });
                        return data;
                    } catch (err) {
                        err.stack = stripErrorStack(err.stack);
                        throw err;
                    }
                },
            };

            if(window.navigator.userAgentData){ // Firefox does not contain this property - to be fixed
                overridePropertyWithProxy(
                    Object.getPrototypeOf(window.navigator.userAgentData),
                    'getHighEntropyValues',
                    getHighEntropyValues,
                );

                overrideInstancePrototype(window.navigator.userAgentData, { brands, mobile, platform });
            }
        } catch (e) {
            console.warn(e);
        }
    };

    function fixWindowChrome(){
        if(isChrome && !window.chrome){
            Object.defineProperty(window, 'chrome', {
                writable: true,
                enumerable: true,
                configurable: false,
                value: {} // incomplete, todo!
            })
        }
    }

    // heavily inspired by https://github.com/berstend/puppeteer-extra/, check it out!
    function fixPermissions(){
        const isSecure = document.location.protocol.startsWith('https')

        if (isSecure) {
            overrideGetterWithProxy(Notification, 'permission', {
                apply() {
                    return 'default'
                }
            });
        }

        if (!isSecure) {
            const handler = {
                apply(target, ctx, args) {
                    const param = (args || [])[0]

                    const isNotifications =
                    param && param.name && param.name === 'notifications'
                    if (!isNotifications) {
                    return utils.cache.Reflect.apply(...arguments)
                    }

                    return Promise.resolve(
                    Object.setPrototypeOf(
                        {
                        state: 'denied',
                        onchange: null
                        },
                        PermissionStatus.prototype
                    )
                    )
                }
            };

            overridePropertyWithProxy(Permissions.prototype, 'query', handler)
        }
    }

    function fixIframeContentWindow(){
        try {
            // Adds a contentWindow proxy to the provided iframe element
            const addContentWindowProxy = iframe => {
              const contentWindowProxy = {
                get(target, key) {
                  if (key === 'self') {
                    return this
                  }
                  if (key === 'frameElement') {
                    return iframe
                  }

                  if (key === '0') {
                    return undefined
                  }
                  return Reflect.get(target, key)
                }
              }

              if (!iframe.contentWindow) {
                const proxy = new Proxy(window, contentWindowProxy)
                Object.defineProperty(iframe, 'contentWindow', {
                  get() {
                    return proxy
                  },
                  set(newValue) {
                    return newValue // contentWindow is immutable
                  },
                  enumerable: true,
                  configurable: false
                })
              }
            }

            // Handles iframe element creation, augments `srcdoc` property so we can intercept further
            const handleIframeCreation = (target, thisArg, args) => {
              const iframe = target.apply(thisArg, args)

              // We need to keep the originals around
              const _iframe = iframe
              const _srcdoc = _iframe.srcdoc

              // Add hook for the srcdoc property
              // We need to be very surgical here to not break other iframes by accident
              Object.defineProperty(iframe, 'srcdoc', {
                configurable: true, // Important, so we can reset this later
                get: function() {
                  return _srcdoc
                },
                set: function(newValue) {
                  addContentWindowProxy(this)
                  // Reset property, the hook is only needed once
                  Object.defineProperty(iframe, 'srcdoc', {
                    configurable: false,
                    writable: false,
                    value: _srcdoc
                  })
                  _iframe.srcdoc = newValue
                }
              })
              return iframe
            }

            // Adds a hook to intercept iframe creation events
            const addIframeCreationSniffer = () => {
              /* global document */
                const createElementHandler = {
                    // Make toString() native
                    get(target, key) {
                    return Reflect.get(target, key)
                    },
                    apply: function(target, thisArg, args) {
                        if (`${args[0]}`.toLowerCase() === 'iframe') {
                            // Everything as usual
                            return handleIframeCreation(target, thisArg, args)
                        }
                        return target.apply(thisArg, args)
                    }
                }

                // All this just due to iframes with srcdoc bug
                overridePropertyWithProxy(
                    document,
                    'createElement',
                    createElementHandler
                )
            }

            // Let's go
            addIframeCreationSniffer()
          } catch (err) {
            // warning message supressed (see https://github.com/apify/fingerprint-suite/issues/61).
            // console.warn(err)
          }
    }

    function fixPluginArray() {
        if(window.navigator.plugins.length !== 0){
            return;
        }

        Object.defineProperty(navigator, 'plugins', {
            get: () => {
                const ChromiumPDFPlugin = Object.create(Plugin.prototype, {
                    description: { value: 'Portable Document Format', enumerable: false },
                    filename: { value: 'internal-pdf-viewer', enumerable: false },
                    name: { value: 'Chromium PDF Plugin', enumerable: false },
                });

                return Object.create(PluginArray.prototype, {
                    length: { value: 1 },
                    0: { value: ChromiumPDFPlugin },
                });
            },
        });
    }

    function runHeadlessFixes(){
        try {
            if( isHeadlessChromium ){
                fixWindowChrome();
                fixPermissions();
                fixIframeContentWindow();
                fixPluginArray();
            }
        } catch (e) {
            console.error(e);
        }
    }

    function overrideStatic(){
        try {
            window.SharedArrayBuffer = undefined;
        } catch (e) {
            console.error(e);
        }
    }

    function inject(fp) {
        const {
            battery,
            navigator: {

                extraProperties,
                userAgentData,
                webdriver,
                ...navigatorProps
            },
            screen: allScreenProps,
            videoCard,
            historyLength,
            audioCodecs,
            videoCodecs,
            mockWebRTC,
            slim,
            // @ts-expect-error internal browser code
        } = fp;

        const {
            // window screen props
            outerHeight,
            outerWidth,
            devicePixelRatio,
            innerWidth,
            innerHeight,
            screenX,
            pageXOffset,
            pageYOffset,

            // Document screen props
            clientWidth,
            clientHeight,
            // Ignore hdr for now.

            hasHDR,
            // window.screen props
            ...newScreen
        } = allScreenProps;

        const windowScreenProps = {
            innerHeight,
            outerHeight,
            outerWidth,
            innerWidth,
            screenX,
            pageXOffset,
            pageYOffset,
            devicePixelRatio,
        };
        const documentScreenProps = {
            clientHeight,
            clientWidth,
        };

        runHeadlessFixes();

        if (mockWebRTC) blockWebRTC();

        if (slim) {
            // @ts-expect-error internal browser code
            // eslint-disable-next-line dot-notation
            window['slim'] = true;
        }

        overrideIntlAPI(navigatorProps.language);
        overrideStatic();

        if (userAgentData) {
            overrideUserAgentData(userAgentData);
        }

        if (window.navigator.webdriver) {
            navigatorProps.webdriver = false;
        }
        overrideInstancePrototype(window.navigator, navigatorProps);

        overrideInstancePrototype(window.screen, newScreen);
        overrideWindowDimensionsProps(windowScreenProps);
        overrideDocumentDimensionsProps(documentScreenProps);

        overrideInstancePrototype(window.history, { length: historyLength });

        overrideWebGl(videoCard);
        overrideCodecs(audioCodecs, videoCodecs);

        overrideBattery(battery);

    }
"""

def create_init_script_with_fingerprint(fingerprint: str):
    return _unclosed_body + f'inject({fingerprint});' + '})()'
