import { describe, it, expect } from "vitest";
import findCompleteBox from "../find_complete_box";

describe("transports utils - findCompleteBox", () => {
  it("should return -1 if the box is not found", () => {
    const byteArr = new Uint8Array([
      0, 0, 0, 9, 0x64, 0x67, 0x32, 0x55, 4, 0, 0, 0, 10, 0x88, 0x68, 0x47, 0x53, 12, 88,
    ]);

    expect(findCompleteBox(byteArr, 0x75757575)).toEqual(-1);
    expect(findCompleteBox(byteArr, 0x99999999)).toEqual(-1);
    expect(findCompleteBox(byteArr, 0x99999)).toEqual(-1);
  });

  it("should return its index if the box is found", () => {
    const byteArr = new Uint8Array([
      0, 0, 0, 9, 0x64, 0x67, 0x32, 0x55, 4, 0, 0, 0, 10, 0x88, 0x68, 0x47, 0x53, 12, 88,
    ]);

    expect(findCompleteBox(byteArr, 0x64673255)).toEqual(0);
    expect(findCompleteBox(byteArr, 0x88684753)).toEqual(9);
  });

  it("should not return a box if it is incomplete", () => {
    const byteArr = new Uint8Array([
      0, 0, 0, 9, 0x64, 0x67, 0x32, 0x55, 4, 0, 0, 0, 10, 0x88, 0x68, 0x47, 0x53, 12,
    ]);

    expect(findCompleteBox(byteArr, 0x88684753)).toEqual(-1);
  });

  it("should return a box if a later one is incomplete", () => {
    const byteArr = new Uint8Array([
      0, 0, 0, 9, 0x64, 0x67, 0x32, 0x55, 4, 0, 0, 0, 12, 0x58, 0x58, 0x57, 0x53, 15, 99,
      87, 77, 0, 0, 0, 10, 0x88, 0x68, 0x47, 0x53, 12,
    ]);

    expect(findCompleteBox(byteArr, 0x64673255)).toEqual(0);
    expect(findCompleteBox(byteArr, 0x58585753)).toEqual(9);
    expect(findCompleteBox(byteArr, 0x88684753)).toEqual(-1);
  });
});
