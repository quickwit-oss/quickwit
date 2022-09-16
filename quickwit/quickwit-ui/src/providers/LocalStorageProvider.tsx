// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

import { createContext, PropsWithChildren, useContext, useEffect, useState } from "react"
import { EMPTY_SEARCH_REQUEST, SearchRequest } from "../utils/models"

type Props = Record<string, unknown>;

type ContextProps = {
  lastSearchRequest: SearchRequest;
  updateLastSearchRequest: (searchRequest: SearchRequest) => void;
};

const defaultValues = {
  lastSearchRequest: EMPTY_SEARCH_REQUEST,
  updateLastSearchRequest: () => undefined,
}

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
  const [lastSearchRequest, setLastSearchRequest] = useState<SearchRequest>(EMPTY_SEARCH_REQUEST);

  useEffect(() => {
    if (localStorage.getItem('lastSearchRequest') !== null) {
      const lastSearchRequest = parseSearchRequest(localStorage.getItem('lastSearchRequest'));
      setLastSearchRequest(lastSearchRequest);
    }
  }, []);

  useEffect(() => {
      localStorage.setItem('lastSearchRequest', JSON.stringify(lastSearchRequest));
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
  )
}

export const useLocalStorage = () => {
  return useContext(LocalStorageContext)
}
