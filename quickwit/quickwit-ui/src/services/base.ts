export class BaseApiClient {
  protected readonly _host: string;
  protected readonly _apiRoot: string;
  constructor(host?:string, apiRoot?:string) {
    this._host = host ? host : window.location.origin;
    this._apiRoot = apiRoot ? apiRoot : "/api/v1/";
  }

  get baseURL(): string {
    return this._host + this._apiRoot;
  }

  getURL(url:string): URL {
    return new URL(this.baseURL+url);
  }

  protected getDefaultGetRequestParams(): RequestInit {
    return {
      method: "GET",
      headers: { Accept: "application/json" },
      mode: "no-cors",
      cache: "default",
    };
  }

  async fetch<T>(url: URL|string, params: RequestInit): Promise<T> {
    const _url = new URL(url, this.baseURL)

    const response = await fetch(_url.href, params);
    if (response.ok) {
      return response.json() as Promise<T>;
    }
    const message = await response.text();
    return await Promise.reject({
      message: message,
      status: response.status
    });
  }

}
