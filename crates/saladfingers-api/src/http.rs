// SPDX-FileCopyrightText: Copyright (C) 2026 Mika Tammi / Pure Fun Solutions
//
// SPDX-License-Identifier: MIT OR Apache-2.0 OR BSD-3-Clause

//! How this crate builds HTTP clients that carry a credential.
//!
//! Every client here sends `Salad-Api-Key` (or, for S4 from inside a container, the IMDS
//! workload JWT). Both are headers, and a header is exactly what a redirect can carry
//! somewhere it should not go — so the policy lives in one place rather than at each
//! `Client::builder()`.

/// How many same-host redirects a credentialed client will follow before giving up.
///
/// A service may legitimately move a path within itself; five hops is plenty for that,
/// and anything longer is a loop.
const MAX_SAME_HOST_REDIRECTS: usize = 5;

/// A [`reqwest::ClientBuilder`] with this workspace's credential-safety policy applied.
/// Callers add their own timeouts on top.
///
/// **Every client that carries a credential must start here.** Two ways a redirect leaks
/// one, both verified against reqwest rather than assumed:
///
/// - **A custom header survives a cross-host redirect.** reqwest strips only the standard
///   sensitive names (`AUTHORIZATION`, `COOKIE`, `PROXY_AUTHORIZATION`, …). `Salad-Api-Key`
///   is custom, so with the default policy — follow up to ten — a single 3xx hands the
///   operator's account-wide key to whatever host it names.
/// - **`Referer` carries the previous URL, query and all.** No URL here embeds a secret
///   today, but S4 object URLs are the kind of thing that grows one, and turning the
///   header off costs nothing.
///
/// The policy is therefore: follow a redirect that stays on the same origin, because a
/// service may legitimately move a path; refuse one that crosses to another origin, by
/// name, because no legitimate case for this crate's endpoints needs it and every case
/// leaks.
///
/// `saladfingers-protocol::transfer::credentialed_client_builder` is the same policy for
/// the presigned-URL side of the system. It is deliberately duplicated rather than shared:
/// this crate is a standalone typed client for SaladCloud's API with no internal
/// dependencies, and importing the CLI↔agent wire-contract crate — and its tar/zstd
/// transfer engine — to reach four lines of HTTP policy would be the worse trade. Keep
/// the two in step.
pub fn credentialed_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .referer(false)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let same_origin = attempt.previous().last().is_some_and(|prev| {
                prev.scheme() == attempt.url().scheme()
                    && prev.host_str() == attempt.url().host_str()
                    && prev.port_or_known_default() == attempt.url().port_or_known_default()
            });
            if !same_origin {
                attempt.error(
                    "refusing a redirect to a different host: this request carries a \
                     credential that would travel with it",
                )
            } else if attempt.previous().len() > MAX_SAME_HOST_REDIRECTS {
                attempt.error("too many redirects")
            } else {
                attempt.follow()
            }
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// `Salad-Api-Key` is a custom header, and reqwest strips only the standard sensitive
    /// ones when a redirect crosses hosts — so with the default policy a single 3xx from
    /// anywhere in the request chain hands the operator's account-wide key to whatever
    /// host it names. A `Client` exposes neither its redirect policy nor its referer
    /// setting, so this is shown by behaviour: the second server must never be reached.
    #[tokio::test]
    async fn a_credentialed_client_refuses_a_cross_host_redirect() {
        let elsewhere = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/collect"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&elsewhere)
            .await;

        let origin = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/away"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", format!("{}/collect", elsewhere.uri())),
            )
            .mount(&origin)
            .await;

        let http = credentialed_client_builder().build().unwrap();
        let err = http
            .get(format!("{}/away", origin.uri()))
            .header("Salad-Api-Key", "sk-operator-secret")
            .send()
            .await
            .expect_err("a cross-host redirect must be refused, not followed");
        assert!(
            format!("{err}").contains("different host") || err.is_redirect(),
            "unexpected error: {err}"
        );
        assert!(
            elsewhere
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty(),
            "the other host was contacted — the API key travelled with the redirect"
        );
    }

    /// Refusing a *same-origin* redirect would be a regression with no security benefit:
    /// the credential is not going anywhere new, and a service may legitimately move a
    /// path within itself.
    #[tokio::test]
    async fn a_same_origin_redirect_is_still_followed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/home"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/arrived"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/arrived"))
            .respond_with(ResponseTemplate::new(200).set_body_string("arrived"))
            .mount(&server)
            .await;

        let http = credentialed_client_builder().build().unwrap();
        let resp = http
            .get(format!("{}/home", server.uri()))
            .send()
            .await
            .expect("a same-origin redirect is legitimate");
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "arrived");

        // The follow that just happened is the ONE request reqwest would ever attach a
        // `Referer` to — the cross-host test can never observe the header, because its
        // other host is rightly never reached. So `referer(false)` is pinned here.
        let requests = server.received_requests().await.unwrap_or_default();
        let arrived: Vec<_> = requests
            .iter()
            .filter(|r| r.url.path() == "/arrived")
            .collect();
        assert_eq!(
            arrived.len(),
            1,
            "exactly one request must land on /arrived"
        );
        assert!(
            !arrived[0].headers.contains_key("referer"),
            "the same-origin follow carried a Referer — `.referer(false)` is gone from \
             the builder"
        );
    }

    /// The loop cap is the one line of the policy the tests above cannot see: a service
    /// redirecting to itself must be cut off at the cap, not followed until reqwest
    /// gives up on its own.
    #[tokio::test]
    async fn a_same_origin_redirect_loop_is_cut_at_the_cap() {
        let server = MockServer::start().await;
        // Self-redirect, with an escape hatch after 25 responses so a broken cap fails
        // the assertions below instead of hanging the test.
        Mock::given(method("GET"))
            .and(path("/loop"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/loop"))
            .up_to_n_times(25)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/loop"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let http = credentialed_client_builder().build().unwrap();
        let err = http
            .get(format!("{}/loop", server.uri()))
            .send()
            .await
            .expect_err("a same-origin redirect loop must be cut at the cap");
        assert!(
            format!("{err}").contains("too many redirects") || err.is_redirect(),
            "unexpected error: {err}"
        );
        assert_eq!(
            server.received_requests().await.unwrap_or_default().len(),
            MAX_SAME_HOST_REDIRECTS + 1,
            "the cap allows the original request plus MAX_SAME_HOST_REDIRECTS follows"
        );
    }
}
