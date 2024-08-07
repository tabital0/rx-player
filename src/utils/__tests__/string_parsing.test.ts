import { describe, it, expect } from "vitest";
import * as strUtils from "../string_parsing";

function checkUint8ArrayEquality(arr1: Uint8Array, arr2: Uint8Array): string | null {
  if (arr1.length !== arr2.length) {
    return `Different length (left: ${arr1.length}, right: ${arr2.length})`;
  }
  for (let i = 0; i < arr1.length; i++) {
    if (arr1[i] !== arr2[i]) {
      return `Different at index ${i} (left: ${arr1[i]}, right: ${arr2[i]})`;
    }
  }
  return null;
}

describe("utils - string parsing", () => {
  describe("strToUtf8", () => {
    it("should return an empty Uint8Array for an empty string", () => {
      const res = strUtils.strToUtf8("");
      expect(res).toHaveLength(0);

      // TODO vitest seems to have issues with `toBeInstanceOf(Uint8Array)`
      expect(res.byteLength).toEqual(0);
      expect(res.buffer.byteLength).toEqual(0);
      expect(res.BYTES_PER_ELEMENT).toEqual(1);
    });

    it("should return an Uint8Array of a regular ASCII string", () => {
      const input = "test";
      const res = strUtils.strToUtf8(input);
      expect(res).toHaveLength(input.length);

      input.split("").forEach((letter, index) => {
        expect(res[index]).toBe(letter.charCodeAt(0));
      });
    });

    it("should return an Uint8Array of the UTF-8 representation of a complex string", () => {
      const input = "t❁ლ";
      const res = strUtils.strToUtf8(input);
      const message = checkUint8ArrayEquality(
        res,
        new Uint8Array([116, 226, 157, 129, 225, 131, 154]),
      );
      if (message !== null) {
        throw new Error(message);
      }
    });
  });

  describe("strToUtf16LE", () => {
    it("should return an empty Uint8Array for an empty string", () => {
      expect(strUtils.strToUtf16LE("")).toEqual(new Uint8Array([]));
    });

    it("should convert a string to little-endian UTF-16 code unit", () => {
      const someLetters = "A❁ლewat";
      expect(strUtils.strToUtf16LE(someLetters)).toEqual(
        new Uint8Array([
          65,
          0, // 0x0041 (A)

          65,
          39, // 0x2741 (❁)

          218,
          16, // 0x10DA (ლ)

          101,
          0, // 0x065 (e)

          119,
          0, // etc.

          97,
          0,

          116,
          0,
        ]),
      );
    });
  });

  describe("strToBeUtf16", () => {
    it("should return an empty Uint8Array for an empty string", () => {
      expect(strUtils.strToBeUtf16("")).toEqual(new Uint8Array([]));
    });

    it("should convert a string to little-endian UTF-16 code unit", () => {
      const someLetters = "A❁ლewat";
      expect(strUtils.strToBeUtf16(someLetters)).toEqual(
        new Uint8Array([
          0,
          65, // 0x0041 (A)

          39,
          65, // 0x2741 (❁)

          16,
          218, // 0x10DA (ლ)

          0,
          101, // 0x065 (e)

          0,
          119, // etc.

          0,
          97,

          0,
          116,
        ]),
      );
    });
  });

  describe("utf16LEToStr", () => {
    it("should return an empty string for an empty Uint8Array", () => {
      expect(strUtils.utf16LEToStr(new Uint8Array([]))).toBe("");
    });

    it("should convert little-endian UTF-16 to its original string", () => {
      const utf16 = new Uint8Array([
        65,
        0, // 0x0041 (A)

        65,
        39, // 0x2741 (❁)

        218,
        16, // 0x10DA (ლ)

        101,
        0, // 0x065 (e)

        119,
        0, // etc.

        97,
        0,

        116,
        0,
      ]);
      expect(strUtils.utf16LEToStr(utf16)).toEqual("A❁ლewat");
    });
  });

  describe("beUtf16ToStr", () => {
    it("should return an empty string for an empty Uint8Array", () => {
      expect(strUtils.beUtf16ToStr(new Uint8Array([]))).toBe("");
    });

    it("should convert little-endian UTF-16 to its original string", () => {
      const utf16 = new Uint8Array([
        0,
        65, // 0x0041 (A)

        39,
        65, // 0x2741 (❁)

        16,
        218, // 0x10DA (ლ)

        0,
        101, // 0x065 (e)

        0,
        119, // etc.

        0,
        97,

        0,
        116,
      ]);
      expect(strUtils.beUtf16ToStr(utf16)).toEqual("A❁ლewat");
    });
  });

  describe("bytesToHex", () => {
    it("should return an empty string for an empty typedArray", () => {
      expect(strUtils.bytesToHex(new Uint8Array([]))).toBe("");
    });

    it("should convert to hexadecimal Uint8Array instances", () => {
      const arr = new Uint8Array([255, 9, 254, 2]);
      expect(strUtils.bytesToHex(arr)).toBe("ff09fe02");
    });

    it("should allow to add a separator", () => {
      const arr = new Uint8Array([255, 9, 254, 2]);
      expect(strUtils.bytesToHex(arr, "--")).toBe("ff--09--fe--02");
    });
  });

  describe("hexToBytes", () => {
    it("should translate an empty string into an empty Uint8Array", () => {
      expect(strUtils.hexToBytes("")).toEqual(new Uint8Array([]));
    });
    it("should translate lower case hexa codes into its Uint8Array counterpart", () => {
      expect(strUtils.hexToBytes("ff87a59800000005")).toEqual(
        new Uint8Array([255, 135, 165, 152, 0, 0, 0, 5]),
      );
    });
    it("should translate higher case hexa codes into its Uint8Array counterpart", () => {
      expect(strUtils.hexToBytes("FECD87A59800000005")).toEqual(
        new Uint8Array([254, 205, 135, 165, 152, 0, 0, 0, 5]),
      );
    });

    it("should translate a mix of higher case and lower case hexa codes into its Uint8Array counterpart", () => {
      expect(strUtils.hexToBytes("FECD87A59800000005")).toEqual(
        new Uint8Array([254, 205, 135, 165, 152, 0, 0, 0, 5]),
      );
    });
  });

  describe("guidToUuid", () => {
    it("should throw if the length is different than 16 bytes", () => {
      expect(() => strUtils.guidToUuid(new Uint8Array(0))).toThrow();
      expect(() => strUtils.guidToUuid(new Uint8Array(4))).toThrow();
      expect(() => strUtils.guidToUuid(new Uint8Array(20))).toThrow();
    });
    it("should translate PlayReady GUID to universal UUID", () => {
      const uuid1 = new Uint8Array([
        15, 27, 175, 76, 7, 184, 156, 73, 181, 133, 213, 230, 192, 48, 134, 31,
      ]);
      const uuid2 = new Uint8Array([
        212, 72, 21, 77, 26, 220, 79, 95, 101, 86, 92, 99, 110, 189, 1, 111,
      ]);
      expect(strUtils.guidToUuid(uuid1)).toEqual(
        new Uint8Array([
          76, 175, 27, 15, 184, 7, 73, 156, 181, 133, 213, 230, 192, 48, 134, 31,
        ]),
      );
      expect(strUtils.guidToUuid(uuid2)).toEqual(
        new Uint8Array([
          77, 21, 72, 212, 220, 26, 95, 79, 101, 86, 92, 99, 110, 189, 1, 111,
        ]),
      );
    });
  });

  describe("utf8ToStr", () => {
    it("should translate nothing by an empty string", () => {
      expect(strUtils.utf8ToStr(new Uint8Array([]))).toBe("");
    });

    it("should translate sequence of UTF-8 code units into the corresponding string", () => {
      expect(
        strUtils.utf8ToStr(
          new Uint8Array([
            0xf0, 0x9f, 0x98, 0x80, 0xf0, 0x9f, 0x90, 0x80, 0xe1, 0xbc, 0x80, 0x65,
          ]),
        ),
      ).toBe("😀🐀ἀe");
    });

    it("should strip off the UTF8 BOM if present", () => {
      expect(
        strUtils.utf8ToStr(
          new Uint8Array([
            0xef, 0xbb, 0xbf, 0xf0, 0x9f, 0x98, 0x80, 0xf0, 0x9f, 0x90, 0x80, 0xe1, 0xbc,
            0x80, 0x65,
          ]),
        ),
      ).toBe("😀🐀ἀe");
    });
  });
});
