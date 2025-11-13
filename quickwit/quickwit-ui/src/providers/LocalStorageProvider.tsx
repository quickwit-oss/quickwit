// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {
  createContext,
  PropsWithChildren,
  useContext,
  useEffect,
  useState,
} from "react";
import { EMPTY_SEARCH_REQUEST, SearchRequest } from "../utils/models";

type Props = Record<string, unknown>;

type ContextProps = {
  lastSearchRequest: SearchRequest;
  updateLastSearchRequest: (searchRequest: SearchRequest) => void;
};

const defaultValues = {
  lastSearchRequest: EMPTY_SEARCH_REQUEST,
  updateLastSearchRequest: () => undefined,
};

function parseSearchRequest(value: string | null): SearchRequest {
  if (value === null) {
    return EMPTY_SEARCH_REQUEST;
  }
  return JSON.parse(value);
}

export const LocalStorageContext = createContext<ContextProps>(defaultValues);

export const LocalStorageProvider = ({
  children,
}: PropsWithChildren<Props>) => {
  const [lastSearchRequest, setLastSearchRequest] =
    useState<SearchRequest>(EMPTY_SEARCH_REQUEST);

  useEffect(() => {
    if (localStorage.getItem("lastSearchRequest") !== null) {
      const lastSearchRequest = parseSearchRequest(
        localStorage.getItem("lastSearchRequest"),
      );
      setLastSearchRequest(lastSearchRequest);
    }
  }, []);

  useEffect(() => {
    localStorage.setItem(
      "lastSearchRequest",
      JSON.stringify(lastSearchRequest),
    );
  }, [lastSearchRequest]);

  function updateLastSearchRequest(searchRequest: SearchRequest) {
    setLastSearchRequest(searchRequest);
  }

  return (
    <LocalStorageContext.Provider
      value={{
        lastSearchRequest,
        updateLastSearchRequest,
      }}
    >
      {children}
    </LocalStorageContext.Provider>
  );
};

export const useLocalStorage = () => {
  return useContext(LocalStorageContext);
};
