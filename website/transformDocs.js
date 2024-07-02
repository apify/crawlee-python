/* eslint-disable */

const fs = require('fs');
const { spawnSync } = require('child_process');

const moduleShortcuts = require('./module_shortcuts.json');

const REPO_ROOT_PLACEHOLDER = 'REPO_ROOT_PLACEHOLDER';

const APIFY_CLIENT_REPO_URL = 'https://github.com/apify/apify-client-python';
const APIFY_SDK_REPO_URL    = 'https://github.com/apify/apify-sdk-python';
const APIFY_SHARED_REPO_URL = 'https://github.com/apify/apify-shared-python';
const CRAWLEE_PYTHON_REPO_URL = 'https://github.com/apify/crawlee-python';

const REPO_URL_PER_PACKAGE = {
    'apify': APIFY_SDK_REPO_URL,
    'apify_client': APIFY_CLIENT_REPO_URL,
    'apify_shared': APIFY_SHARED_REPO_URL,
    'crawlee': CRAWLEE_PYTHON_REPO_URL,
};

// For each package, get the installed version, and set the tag to the corresponding version
const TAG_PER_PACKAGE = {};
for (const pkg of ['apify', 'apify_client', 'apify_shared']) {
    const spawnResult = spawnSync('python', ['-c', `import ${pkg}; print(${pkg}.__version__)`]);
    if (spawnResult.status === 0) {
        TAG_PER_PACKAGE[pkg] = `v${spawnResult.stdout.toString().trim()}`;
    }
}

// For the current package, set the tag to 'master'
const thisPackagePyprojectToml = fs.readFileSync('../pyproject.toml', 'utf8');
const thisPackageName = thisPackagePyprojectToml.match(/^name = "(.+)"$/m)[1];
TAG_PER_PACKAGE[thisPackageName] = 'master';


// Taken from https://github.com/TypeStrong/typedoc/blob/v0.23.24/src/lib/models/reflections/kind.ts, modified
const TYPEDOC_KINDS = {
    'class': {
        kind: 128,
        kindString: 'Class',
    },
    'function': {
        kind: 2048,
        kindString: 'Method',
    },
    'data': {
        kind: 1024,
        kindString: 'Property',
    },
    'enum': {
        kind: 8,
        kindString: 'Enumeration',
    },
    'enumValue': {
        kind: 16,
        kindString: 'Enumeration Member',
    },
}

const GROUP_ORDER = [
    'Main Classes',
    'Helper Classes',
    'Errors',
    'Constructors',
    'Methods',
    'Properties',
    'Constants',
    'Enumeration Members'
];

const groupSort = (g1, g2) => {
    if(GROUP_ORDER.includes(g1) && GROUP_ORDER.includes(g2)){
        return GROUP_ORDER.indexOf(g1) - GROUP_ORDER.indexOf(g2)
    }
    return g1.localeCompare(g2);
};

function getGroupName(object) {
    const groupPredicates = {
        'Errors': (x) => x.name.toLowerCase().includes('error'),
        'Main Classes': (x) => ['Dataset', 'KeyValueStore', 'RequestQueue'].includes(x.name) || x.name.endsWith('Crawler'),
        'Helper Classes': (x) => x.kindString === 'Class',
        'Methods': (x) => x.kindString === 'Method',
        'Constructors': (x) => x.kindString === 'Constructor',
        'Properties': (x) => x.kindString === 'Property',
        'Constants': (x) => x.kindString === 'Enumeration',
        'Enumeration Members': (x) => x.kindString === 'Enumeration Member',
    };

    const [group] = Object.entries(groupPredicates).find(
        ([_, predicate]) => predicate(object)
    );

    return group;
}

// Strips the Optional[] type from the type string, and replaces generic types with just the main type
function getBaseType(type) {
    return type?.replace(/Optional\[(.*)\]/g, '$1').replace('ListPage[Dict]', 'ListPage');
}

// Returns whether a type is a custom class, or a primitive type
function isCustomClass(type) {
    return !['dict', 'list', 'str', 'int', 'float', 'bool'].includes(type.toLowerCase());
}

// Infer the Typedoc type from the docspec type
function inferTypedocType(docspecType) {
    const typeWithoutOptional = getBaseType(docspecType);
    if (!typeWithoutOptional) {
        return undefined;
    }

    // Typically, if a type is a custom class, it will be a reference in Typedoc
    return isCustomClass(typeWithoutOptional) ? {
        type: 'reference',
        name: docspecType
    } : {
        type: 'intrinsic',
        name: docspecType,
    }
}

// Sorts the groups of a Typedoc member, and sorts the children of each group
function sortChildren(typedocMember) {
    for (let group of typedocMember.groups) {
        group.children
            .sort((a, b) => {
                const firstName = typedocMember.children.find(x => x.id === a).name;
                const secondName = typedocMember.children.find(x => x.id === b).name;
                return firstName.localeCompare(secondName);
            });
    }
    typedocMember.groups.sort((a, b) => groupSort(a.title, b.title));
}

// Parses the arguments and return value description of a method from its docstring
function extractArgsAndReturns(docstring) {
    const parameters = (docstring
        .split('Args:')[1] ?? '').split('Returns:')[0] // Get the part between Args: and Returns:
        .split(/(^|\n)\s*([\w]+)\s*\(.*?\)\s*:\s*/) // Magic regex which splits the arguments into an array, and removes the argument types
        .filter(x => x.length > 1) // Remove empty strings
        .reduce((acc, curr, idx, arr) => { // Collect the argument names and types into an object
            if(idx % 2 === 0){
                return {...acc, [curr]: arr[idx+1]} // If the index is even, the current string is an argument name, and the next string is its type
            }
            return acc;
        }, {});

    const returns = (docstring
        .split('Returns:')[1] ?? '').split('Raises:')[0] // Get the part between Returns: and Raises:
        .split(':')[1]?.trim() || undefined; // Split the return value into its type and description, return description


    return { parameters, returns };
}

// Objects with decorators named 'ignore_docs' or with empty docstrings will be ignored
function isHidden(member) {
    return member.decorations?.some(d => d.name === 'ignore_docs') || member.name === 'ignore_docs';
}

// Each object in the Typedoc structure has an unique ID,
// we'll just increment it for each object we convert
let oid = 1;

const symbolIdMap = [];

// Converts a docspec object to a Typedoc object, including all its children
function convertObject(obj, parent, module) {
    const rootModuleName = module.name.split('.')[0];
    for (let member of obj.members ?? []) {
        let typedocKind = TYPEDOC_KINDS[member.type];

        if(member.bases?.includes('Enum')) {
            typedocKind = TYPEDOC_KINDS['enum'];
        }

        let typedocType = inferTypedocType(member.datatype);
        
        if (member.decorations?.some(d => ['property', 'dualproperty'].includes(d.name))) {
            typedocKind = TYPEDOC_KINDS['data'];
            typedocType = inferTypedocType(member.return_type ?? member.datatype);
        }
        
        if(parent.kindString === 'Enumeration') {
            typedocKind = TYPEDOC_KINDS['enumValue'];
            typedocType = {
                type: 'literal',
                value: member.value,
            }
        }

        if(member.type in TYPEDOC_KINDS && !isHidden(member)) {
            // Get the URL of the member in GitHub
            const repoBaseUrl = `${REPO_URL_PER_PACKAGE[rootModuleName]}/blob/${TAG_PER_PACKAGE[rootModuleName]}`;
            const filePathInRepo = member.location.filename.replace(REPO_ROOT_PLACEHOLDER, '');
            const fileGitHubUrl = member.location.filename.replace(REPO_ROOT_PLACEHOLDER, repoBaseUrl);
            const memberGitHubUrl = `${fileGitHubUrl}#L${member.location.lineno}`;

            symbolIdMap.push({
                qualifiedName: member.name,
                sourceFileName: filePathInRepo,
            });

            // Get the module name of the member, and check if it has a shortcut (reexport from an ancestor module)
            const fullName = `${module.name}.${member.name}`;
            let moduleName = module.name;
            if (fullName in moduleShortcuts) {
                moduleName = moduleShortcuts[fullName].replace(`.${member.name}`, '');
            }

            // Create the Typedoc member object
            let typedocMember = {
                id: oid++,
                name: member.name,
                module: moduleName, // This is an extension to the original Typedoc structure, to support showing where the member is exported from
                ...typedocKind,
                flags: {},
                comment: member.docstring ? {
                    summary: [{
                        kind: 'text',
                        text: member.docstring?.content,
                    }],
                } : undefined,
                type: typedocType,
                children: [],
                groups: [],
                sources: [{
                    filename: filePathInRepo,
                    line: member.location.lineno,
                    character: 1,
                    url: memberGitHubUrl,
                }],
            };

            if(typedocMember.kindString === 'Method') {
                const { parameters, returns } = extractArgsAndReturns(member.docstring?.content ?? '');

                typedocMember.signatures = [{
                    id: oid++,
                    name: member.name,
                    modifiers: member.modifiers ?? [],
                    kind: 4096,
                    kindString: 'Call signature',
                    flags: {},
                    comment: member.docstring ? {
                        summary: [{
                            kind: 'text',
                            text: member.docstring?.content
                                .replace(/\**(Args|Arguments|Returns)[\s\S]+/, ''),
                        }],
                        blockTags: returns ? [
                            { tag: '@returns', content: [{ kind: 'text', text: returns }] },
                        ] : undefined,
                    } : undefined,
                    type: inferTypedocType(member.return_type),
                    parameters: member.args.filter((arg) => (arg.name !== 'self' && arg.name !== 'cls')).map((arg) => ({
                        id: oid++,
                        name: arg.name,
                        kind: 32768,
                        kindString: 'Parameter',
                        flags: {
                            isOptional: arg.datatype?.includes('Optional') ? 'true' : undefined,
                            'keyword-only': arg.type === 'KEYWORD_ONLY' ? 'true' : undefined,
                        },
                        type: inferTypedocType(arg.datatype),
                        comment: parameters[arg.name] ? {
                            summary: [{
                                kind: 'text',
                                text: parameters[arg.name]
                            }]
                        } : undefined,
                        defaultValue: arg.default_value,
                    })),
                }];
            }

            if(typedocMember.name === '__init__') {
                typedocMember.kind = 512;
                typedocMember.kindString = 'Constructor';
            }

            convertObject(member, typedocMember, module);

            const groupName = getGroupName(typedocMember);

            const group = parent.groups.find((g) => g.title === groupName);
            if (group) {
                group.children.push(typedocMember.id);
            } else {
                parent.groups.push({
                    title: groupName,
                    children: [typedocMember.id],
                });
            }

            sortChildren(typedocMember);
            parent.children.push(typedocMember);
        }
    }
}

function main() {
    // Root object of the Typedoc structure
    const typedocApiReference = {
        'id': 0,
        'name': 'apify-client',
        'kind': 1,
        'kindString': 'Project',
        'flags': {},
        'originalName': '',
        'children': [],
        'groups': [],
        'sources': [
            {
                'fileName': 'src/index.ts',
                'line': 1,
                'character': 0,
                'url': `http://example.com/blob/123456/src/dummy.py`,
            }
        ]
    };

    // Load the docspec dump files of this module and of apify-shared
    const thisPackageDocspecDump = fs.readFileSync('docspec-dump.jsonl', 'utf8');
    const thisPackageModules = thisPackageDocspecDump.split('\n').filter((line) => line !== '');

    // const apifySharedDocspecDump = fs.readFileSync('apify-shared-docspec-dump.jsonl', 'utf8');
    // const apifySharedModules = apifySharedDocspecDump.split('\n').filter((line) => line !== '');

    // Convert all the modules, store them in the root object
    for (const module of [...thisPackageModules]) {
        const parsedModule = JSON.parse(module);
        convertObject(parsedModule, typedocApiReference, parsedModule);
    };

    // Recursively fix references (collect names->ids of all the named entities and then inject those in the reference objects)
    const namesToIds = {};
    function collectIds(obj) {
        for (const child of obj.children ?? []) {
            namesToIds[child.name] = child.id;
            collectIds(child);
        }
    }
    collectIds(typedocApiReference);

    function fixRefs(obj) {
        for (const child of obj.children ?? []) {
            if (child.type?.type === 'reference') {
                child.type.id = namesToIds[child.type.name];
            }
            if (child.signatures) {
                for (const sig of child.signatures) {
                    for (const param of sig.parameters ?? []) {
                        if (param.type?.type === 'reference') {
                            param.type.id = namesToIds[param.type.name];
                        }
                    }
                    if (sig.type?.type === 'reference') {
                        sig.type.id = namesToIds[sig.type.name];
                    }
                }
            }
            fixRefs(child);
        }
    }
    fixRefs(typedocApiReference);

    // Sort the children of the root object
    sortChildren(typedocApiReference);

    typedocApiReference.symbolIdMap = Object.fromEntries(Object.entries(symbolIdMap));

    // Write the Typedoc structure to the output file
    fs.writeFileSync('./api-typedoc-generated.json', JSON.stringify(typedocApiReference, null, 4));
}

if (require.main === module) {
    main();
}

module.exports = {
    groupSort,
}
