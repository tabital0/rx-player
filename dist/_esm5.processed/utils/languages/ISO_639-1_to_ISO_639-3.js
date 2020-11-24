/**
 * Copyright 2015 CANAL+ Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Translate ISO 639-1 language codes into ISO 639-3 ones.
 */
var ISO_MAP_1_TO_3 = {
    aa: "aar",
    ab: "abk",
    ae: "ave",
    af: "afr",
    ak: "aka",
    am: "amh",
    an: "arg",
    ar: "ara",
    as: "asm",
    av: "ava",
    ay: "aym",
    az: "aze",
    ba: "bak",
    be: "bel",
    bg: "bul",
    bi: "bis",
    bm: "bam",
    bn: "ben",
    bo: "bod",
    br: "bre",
    bs: "bos",
    ca: "cat",
    ce: "che",
    ch: "cha",
    co: "cos",
    cr: "cre",
    cs: "ces",
    cu: "chu",
    // Old Slavonic, Old Bulgarian
    cv: "chv",
    cy: "cym",
    da: "dan",
    de: "deu",
    dv: "div",
    dz: "dzo",
    ee: "ewe",
    el: "ell",
    en: "eng",
    eo: "epo",
    es: "spa",
    et: "est",
    eu: "eus",
    fa: "fas",
    ff: "ful",
    fi: "fin",
    fj: "fij",
    fo: "fao",
    fr: "fra",
    fy: "fry",
    ga: "gle",
    gd: "gla",
    gl: "glg",
    gn: "grn",
    gu: "guj",
    gv: "glv",
    ha: "hau",
    he: "heb",
    hi: "hin",
    ho: "hmo",
    hr: "hrv",
    ht: "hat",
    hu: "hun",
    hy: "hye",
    hz: "her",
    ia: "ina",
    id: "ind",
    ie: "ile",
    ig: "ibo",
    ii: "iii",
    ik: "ipk",
    io: "ido",
    is: "isl",
    it: "ita",
    iu: "iku",
    ja: "jpn",
    jv: "jav",
    ka: "kat",
    kg: "kon",
    ki: "kik",
    kj: "kua",
    kk: "kaz",
    kl: "kal",
    km: "khm",
    kn: "kan",
    ko: "kor",
    kr: "kau",
    ks: "kas",
    ku: "kur",
    kv: "kom",
    kw: "cor",
    ky: "kir",
    la: "lat",
    lb: "ltz",
    lg: "lug",
    li: "lim",
    ln: "lin",
    lo: "lao",
    lt: "lit",
    lu: "lub",
    lv: "lav",
    mg: "mlg",
    mh: "mah",
    mi: "mri",
    mk: "mkd",
    ml: "mal",
    mn: "mon",
    mr: "mar",
    ms: "msa",
    mt: "mlt",
    my: "mya",
    na: "nau",
    nb: "nob",
    nd: "nde",
    ne: "nep",
    ng: "ndo",
    nl: "nld",
    nn: "nno",
    no: "nor",
    nr: "nbl",
    nv: "nav",
    ny: "nya",
    oc: "oci",
    oj: "oji",
    om: "orm",
    or: "ori",
    os: "oss",
    pa: "pan",
    pi: "pli",
    pl: "pol",
    ps: "pus",
    pt: "por",
    qu: "que",
    rm: "roh",
    rn: "run",
    ro: "ron",
    ru: "rus",
    rw: "kin",
    sa: "san",
    sc: "srd",
    sd: "snd",
    se: "sme",
    sg: "sag",
    si: "sin",
    sk: "slk",
    sl: "slv",
    sm: "smo",
    sn: "sna",
    so: "som",
    sq: "sqi",
    sr: "srp",
    ss: "ssw",
    st: "sot",
    su: "sun",
    sv: "swe",
    sw: "swa",
    ta: "tam",
    te: "tel",
    tg: "tgk",
    th: "tha",
    ti: "tir",
    tk: "tuk",
    tl: "tgl",
    tn: "tsn",
    to: "ton",
    tr: "tur",
    ts: "tso",
    tt: "tat",
    tw: "twi",
    ty: "tah",
    ug: "uig",
    uk: "ukr",
    ur: "urd",
    uz: "uzb",
    ve: "ven",
    vi: "vie",
    vo: "vol",
    wa: "wln",
    wo: "wol",
    xh: "xho",
    yi: "yid",
    yo: "yor",
    za: "zha",
    zh: "zho",
    zu: "zul",
};
export default ISO_MAP_1_TO_3;
