# Meta for Developers Setup for IG Tag Puller

One-time setup (~30 min) to get the credentials the IG tag puller needs. Tokens expire every ~60 days, so save these instructions for re-running when that happens.

## Prereqs

- @bigfundj is an Instagram Business Account. ✓ Confirmed.
- @bigfundj is connected to your Big Fun Disc Jockeys Facebook Page. ✓ Confirmed.
- You're an admin of that Facebook Page. (If you're not, you can't generate tokens — get admin access first.)

## Steps

### 1. Create a Meta for Developers app

1. Go to https://developers.facebook.com/apps/
2. Log in with the Facebook account that admins the BIG FUN Page.
3. Click **Create App**.
   - **Use case:** "Other"
   - **App type:** "Business"
   - **App name:** `BIG FUN IG Ingester` (only visible to you)
   - **Contact email:** your email
   - **Business portfolio:** skip if it asks, or pick yours

You'll land on the app dashboard. Note the **App ID** at the top of the page. You'll need it later.

### 2. Add the Instagram product

1. From the app dashboard, scroll to **Add products to your app**.
2. Find **Instagram** and click **Set up**. (It's sometimes labeled "Instagram Graph API" or "Instagram for Business" depending on Meta's current UI naming.)
3. If prompted to "Configure Webhooks" or "Add Use Case," skip / dismiss. We're using API polling, not webhooks.

### 3. Find your App Secret

1. Left sidebar: **App settings** → **Basic**.
2. Next to **App secret**, click **Show**, log in to confirm. Copy it. Save in 1Password as `Meta App Secret`.

### 4. Get your IG Business Account ID and Page Access Token

This is the most clicks. Use Graph API Explorer.

1. Go to https://developers.facebook.com/tools/explorer/
2. Top-right dropdown: pick your `BIG FUN IG Ingester` app.
3. Just below that, **User or Page** dropdown: click and select **Get User Access Token**.
4. A permissions panel appears. Tick these (and only these):
   - `pages_show_list`
   - `pages_read_engagement`
   - `instagram_basic`
   - `instagram_manage_insights`
   - `business_management` (only if it appears in the list — not always required)
5. Click **Generate Access Token**. A Facebook permissions popup will ask you to grant access. Approve.
6. The Access Token field at the top is now populated. Don't close this tab.
7. In the query box (the field where you'd type a path), enter:
   ```
   me/accounts
   ```
   Click **Submit**. The response lists Pages you admin. Find your BIG FUN Disc Jockeys page entry and copy the `id` (a long number). Save as `Page ID`.
8. Now query (replace the long number with your Page ID):
   ```
   YOUR_PAGE_ID?fields=instagram_business_account
   ```
   The response includes `instagram_business_account.id`. Save as `IG Business Account ID`.
9. Now query:
   ```
   YOUR_PAGE_ID?fields=access_token
   ```
   The response includes `access_token`. **This is the Page Access Token.** It's short-lived (~1 hour) right now. We'll exchange it next.

### 5. Exchange for a long-lived Page Access Token

The Page Access Token from step 4 only lasts an hour. Long-lived tokens last 60 days, which is what the script needs.

In Graph API Explorer, run this query (paste the values inline — replace the placeholders):

```
oauth/access_token?grant_type=fb_exchange_token&client_id=YOUR_APP_ID&client_secret=YOUR_APP_SECRET&fb_exchange_token=YOUR_SHORT_LIVED_USER_TOKEN
```

Where:
- `YOUR_APP_ID` = from step 1
- `YOUR_APP_SECRET` = from step 3
- `YOUR_SHORT_LIVED_USER_TOKEN` = the User Access Token currently shown at the top of the Graph API Explorer (from step 4 step 5)

Click **Submit**. The response gives you a long-lived **User Access Token** (60-day TTL).

Now query (replace placeholders):
```
YOUR_PAGE_ID?fields=access_token&access_token=YOUR_LONG_LIVED_USER_TOKEN
```

The response includes a long-lived **Page Access Token**. Page Access Tokens derived from a long-lived User Access Token don't expire — they last as long as the user doesn't revoke permissions or change password.

Save this as `IG Page Access Token`. This is the token the script uses.

### 6. Save these values somewhere safe

You'll need them again in 60 days when the token expires (well, technically a non-expiring page token doesn't expire, but Meta still revokes them periodically — assume rotation every 60 days).

| Variable | Where it came from |
|---|---|
| `META_APP_ID` | App dashboard top |
| `META_APP_SECRET` | App settings → Basic |
| `IG_BUSINESS_ACCOUNT_ID` | Step 4.8 |
| `IG_PAGE_ACCESS_TOKEN` | Step 5 |

Put these in 1Password (or wherever you keep credentials), labeled "Meta IG Ingester."

### 7. Hand off to me

Once you have those four values, the next step is wiring them into:
1. A local `.env` file in the bigfun-dashboard repo (for local testing — gitignored)
2. GitHub Secrets in the bigfun-dashboard repo (for the daily Action)

I'll walk you through both when you're ready. Don't paste the actual values into chat — we'll do it through the gh CLI or GitHub web UI.

## Token expiration handling

The script will check token validity on each run. If the token has been revoked (Meta auth changes, password change, etc.), it'll fail loudly and email/notify (TBD on notification mechanism). When that happens, repeat steps 4-5 to get a fresh token. You don't need to redo steps 1-3 unless the app itself is deleted.

## If you get stuck

The Meta dev UI changes frequently. If a step doesn't match what you see:
- The official docs are at https://developers.facebook.com/docs/instagram-api/getting-started
- The most common confusion is "Instagram Basic Display" (deprecated, don't use) vs "Instagram Graph API" (what we want).
- If you see permission errors, double-check the IG account is set as **Business** (not Creator) and is connected to a Facebook Page.
