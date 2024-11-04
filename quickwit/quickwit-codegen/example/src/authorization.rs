// The Quickwit Enterprise Edition (EE) license
// Copyright (c) 2024-present Quickwit Inc.
//
// With regard to the Quickwit Software:
//
// This software and associated documentation files (the "Software") may only be
// used in production, if you (and any entity that you represent) hold a valid
// Quickwit Enterprise license corresponding to your usage.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use quickwit_authorize::{Authorization, AuthorizationError, AuthorizationToken, StreamAuthorization};

use crate::{GoodbyeRequest, HelloRequest, PingRequest};

impl Authorization for HelloRequest {
    fn attenuate(
        &self,
        auth_token: AuthorizationToken,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl Authorization for GoodbyeRequest {
    fn attenuate(
        &self,
        auth_token: AuthorizationToken,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}

impl StreamAuthorization for PingRequest {
    fn attenuate(
        auth_token: AuthorizationToken,
    ) -> Result<AuthorizationToken, AuthorizationError> {
        Ok(auth_token)
    }
}
