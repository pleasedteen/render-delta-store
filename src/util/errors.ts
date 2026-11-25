/** 带 HTTP 状态码的业务错误，路由层统一捕获。 */
export class HttpError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.code = code;
  }
}

export const badRequest = (msg: string) => new HttpError(400, 'bad_request', msg);
export const notFound = (msg: string) => new HttpError(404, 'not_found', msg);
export const conflict = (msg: string) => new HttpError(409, 'conflict', msg);
export const gone = (msg: string) => new HttpError(410, 'gone', msg);
export const payloadTooLarge = (msg: string) => new HttpError(413, 'payload_too_large', msg);
export const unprocessable = (msg: string) => new HttpError(422, 'unprocessable', msg);
