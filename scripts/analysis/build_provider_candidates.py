#!/usr/bin/env python3
"""
Build provider candidate dicts from historical reports, merge into lookup tables.
Existing entries always win — this only fills gaps.

Usage:
  python3 scripts/analysis/build_provider_candidates.py [--dry-run]
"""

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent
MX_TABLE   = REPO / 'data/mappings/mx_providers.json'
SPF_TABLE  = REPO / 'data/mappings/spf_providers.json'

MX_HIST_REPORT  = REPO / 'results/top_mx_historical/top_mx_historical_both_20260602_214107.txt'
SPF_HIST_REPORT = REPO / 'results/top_spf_historical/top_spf_historical_both_20260602_230810.txt'

THRESHOLD = 100  # min domains to include

# ---------------------------------------------------------------------------
# Known MX provider annotations  (domain → {provider, confidence})
# ---------------------------------------------------------------------------
MX_KNOWN = {
    # Large mystery family — bulk/disposable mail infra, no named operator
    "mb1p.com":           {"provider": "Unknown bulk mail infrastructure", "confidence": 25},
    "m2bp.com":           {"provider": "Unknown bulk mail infrastructure", "confidence": 25},
    "mb2p.com":           {"provider": "Unknown bulk mail infrastructure", "confidence": 25},
    "mb5p.com":           {"provider": "Unknown bulk mail infrastructure", "confidence": 25},
    "m1bp.com":           {"provider": "Unknown bulk mail infrastructure", "confidence": 25},
    # Named / researched
    "hostedmxserver.com": {"provider": "Tucows OpenSRS", "confidence": 75},
    "amenworld.com":      {"provider": "Amen (team.blue)", "confidence": 85},
    "fsdata.se":          {"provider": "FS Data (Miss Group)", "confidence": 80},
    "replyingback.com":   {"provider": "Unknown (likely retired)", "confidence": 20},
    "webnode.com":        {"provider": "Webnode", "confidence": 85},
    "tba.net":            {"provider": "Internet.se (TBA Media)", "confidence": 75},
    "schlund.de":         {"provider": "IONOS (legacy Schlund+Partner)", "confidence": 90},
    "b-io.co":            {"provider": "bounce.io (bounce handling)", "confidence": 50},
    "hostedmxserver.com": {"provider": "Tucows OpenSRS", "confidence": 75},
    "misshosting.com":    {"provider": "Miss Hosting (Miss Group)", "confidence": 85},
    "orange.fr":          {"provider": "Orange France", "confidence": 90},
    "zone.eu":            {"provider": "Zone Media", "confidence": 80},
    "surftown.se":        {"provider": "Surftown", "confidence": 85},
    "surf-town.net":      {"provider": "Surftown", "confidence": 85},
    "surftown.dk":        {"provider": "Surftown", "confidence": 85},
    "antagonist.nl":      {"provider": "Antagonist", "confidence": 85},
    "unoeuro.com":        {"provider": "UnoEuro (One.com)", "confidence": 85},
    "linkeo.org":         {"provider": "Linkeo", "confidence": 75},
    "telia.com":          {"provider": "Telia", "confidence": 90},
    "telia.ee":           {"provider": "Telia Estonia", "confidence": 90},
    "elion.ee":           {"provider": "Elion / Telia Estonia", "confidence": 85},
    "schlund.de":         {"provider": "IONOS (legacy Schlund+Partner)", "confidence": 90},
    "crystone.se":        {"provider": "Crystone", "confidence": 85},
    "crystone.net":       {"provider": "Crystone", "confidence": 85},
    "ballou.se":          {"provider": "Ballou", "confidence": 80},
    "nickstel.com":       {"provider": "Unknown (disposable mail)", "confidence": 20},
    "in-mx.net":          {"provider": "Unknown (backup MX relay)", "confidence": 30},
    "in-mx.com":          {"provider": "Unknown (backup MX relay)", "confidence": 30},
    "planbnow.co":        {"provider": "Unknown", "confidence": 20},
    "reliablemail.org":   {"provider": "Reliable Mail", "confidence": 65},
    "serveriai.lt":       {"provider": "Serveriai.lt (Lithuanian hosting)", "confidence": 75},
    "123hjemmeside.dk":   {"provider": "123 Hjemmeside (One.com)", "confidence": 80},
    "svenskadomaner.se":  {"provider": "Svenska Domäner", "confidence": 80},
    "levonline.com":      {"provider": "Levonline", "confidence": 80},
    "parking.se":         {"provider": "Parking.se (domain parking)", "confidence": 70},
    "egensajt.se":        {"provider": "Egensajt", "confidence": 75},
    "simplesite.com":     {"provider": "SimpleSite", "confidence": 85},
    "webfaction.com":     {"provider": "WebFaction (GoDaddy)", "confidence": 80},
    "axc.eu":             {"provider": "AXC", "confidence": 65},
    "axc.nl":             {"provider": "AXC", "confidence": 65},
    "sitew.fr":           {"provider": "SiteW", "confidence": 80},
    "elion.ee":           {"provider": "Elion / Telia Estonia", "confidence": 85},
    "nazwa.pl":           {"provider": "Nazwa.pl", "confidence": 80},
    "blacknight.com":     {"provider": "Blacknight", "confidence": 85},
    "iinet.net.au":       {"provider": "iiNet (Australia)", "confidence": 85},
    "hushmail.com":       {"provider": "Hushmail", "confidence": 90},
    "mailo.com":          {"provider": "Mailo", "confidence": 85},
    "ouvaton.coop":       {"provider": "Ouvaton", "confidence": 80},
    "canit.ca":           {"provider": "CanIt (spam filtering)", "confidence": 75},
    "btconnect.com":      {"provider": "BT Business", "confidence": 85},
    "ipage.com":          {"provider": "iPage (Newfold Digital)", "confidence": 85},
    "mandic.com.br":      {"provider": "Mandic (Brazil)", "confidence": 80},
    "redehost.com.br":    {"provider": "RedeHost (Brazil)", "confidence": 80},
    "superhost.pl":       {"provider": "Super-Host.pl", "confidence": 75},
    "super-host.pl":      {"provider": "Super-Host.pl", "confidence": 75},
    "dotmailer.co.uk":    {"provider": "Dotdigital (legacy)", "confidence": 80},
    "spamina.com":        {"provider": "Spamina (email security)", "confidence": 80},
    "exchangedefender.com": {"provider": "ExchangeDefender", "confidence": 80},
    "mcsv.net":           {"provider": "Mailchimp", "confidence": 90},
    "rsgsv.net":          {"provider": "Mailchimp", "confidence": 90},
    "reflexion.net":      {"provider": "Reflexion (Sophos)", "confidence": 80},
    "sfrbusinessteam.fr": {"provider": "SFR Business (French telecom)", "confidence": 85},
    "register.it":        {"provider": "Register.it (team.blue)", "confidence": 85},
    "gigahost.dk":        {"provider": "Gigahost", "confidence": 80},
    "zitcom.dk":          {"provider": "Zitcom", "confidence": 80},
    "scannet.dk":         {"provider": "Scannet", "confidence": 80},
    "comendosystems.com": {"provider": "Comendo (email security)", "confidence": 80},
    "comendosystems.net": {"provider": "Comendo (email security)", "confidence": 80},
    "1and1.mx":           {"provider": "IONOS (legacy 1&1)", "confidence": 90},
    "1and1.it":           {"provider": "IONOS (legacy 1&1)", "confidence": 90},
    "inetadmin.eu":       {"provider": "InetAdmin", "confidence": 70},
    "inetadmin.sk":       {"provider": "InetAdmin Slovakia", "confidence": 70},
    "inetadmin.cz":       {"provider": "InetAdmin Czech", "confidence": 70},
    "nexylan.net":        {"provider": "Nexylan (French hosting)", "confidence": 75},
    "phpnet.org":         {"provider": "PHPnet (French hosting)", "confidence": 75},
    "suprabox.net":       {"provider": "Suprabox (French hosting)", "confidence": 75},
    "binero.se":          {"provider": "Binero", "confidence": 80},
    "youcan.shop":        {"provider": "YouCan.Shop (ecommerce)", "confidence": 80},
    "servage.net":        {"provider": "Servage", "confidence": 75},
    "webador.com":        {"provider": "Webador", "confidence": 85},
    "axecibles.com":      {"provider": "Axecibles (French web agency)", "confidence": 70},
    "nfrance.com":        {"provider": "NFrance (French hosting)", "confidence": 75},
    "domainoo.fr":        {"provider": "Domainoo (French hosting)", "confidence": 75},
    "web4u.cz":           {"provider": "Web4U (Czech hosting)", "confidence": 75},
    "aha.ru":             {"provider": "Aha.ru (Russian hosting)", "confidence": 70},
    "vargonen.net":       {"provider": "Vargonen (Turkish hosting)", "confidence": 75},
    "freehostia.com":     {"provider": "FreeHostia", "confidence": 75},
    "la-boite-immo.fr":   {"provider": "La Boite Immo (French real estate SaaS)", "confidence": 75},
    "spamfilter.gr":      {"provider": "SpamFilter.gr (Greek email security)", "confidence": 70},
    "nebula.fi":          {"provider": "Nebula (Finnish hosting)", "confidence": 80},
    "hostcontrol.com":    {"provider": "HostControl", "confidence": 65},
    "jouwweb.nl":         {"provider": "JouwWeb (Dutch website builder)", "confidence": 80},
    "web.de":             {"provider": "WEB.DE (German email)", "confidence": 90},
    "dyndns.org":         {"provider": "Dyn (Oracle)", "confidence": 75},
    "efty.com":           {"provider": "Efty (domain portfolio)", "confidence": 65},
    "iserv.eu":           {"provider": "iServ (German school IT)", "confidence": 80},
    "infobox.ru":         {"provider": "Infobox (Russian hosting)", "confidence": 75},
    "peterhost.ru":       {"provider": "PeterHost (Russian hosting)", "confidence": 70},
    "webasyst.com":       {"provider": "Webasyst (Russian ecommerce)", "confidence": 75},
    "appiancloud.com":    {"provider": "Appian (BPM platform)", "confidence": 75},
    "parklogic.com":      {"provider": "ParkLogic (domain parking)", "confidence": 70},
    "expurgate.de":       {"provider": "Retarus Expurgate (email security)", "confidence": 80},
    "hostingservice.fi":  {"provider": "HostingService.fi (Finnish hosting)", "confidence": 70},
    "haisoft.net":        {"provider": "Haisoft (French hosting)", "confidence": 70},
    "planet-work.com":    {"provider": "Planet-Work (French hosting)", "confidence": 70},
    "ddn.fr":             {"provider": "DDN.fr (French hosting)", "confidence": 65},
    "oxatis.com":         {"provider": "Oxatis (French ecommerce)", "confidence": 80},
    "viaduc.fr":          {"provider": "Viaduc (French hosting)", "confidence": 70},
    "tornado.email":      {"provider": "Tornado Email", "confidence": 65},
    "msgfocus.com":       {"provider": "Adestra / MsgFocus (email marketing)", "confidence": 75},
    "is.nl":              {"provider": "IS (Dutch hosting)", "confidence": 65},
    "oml.ru":             {"provider": "OML.ru (Russian hosting)", "confidence": 65},
    "wk.se":              {"provider": "WK (Swedish hosting)", "confidence": 65},
    "inviso.se":          {"provider": "Inviso (Swedish)", "confidence": 65},
    "byte.nl":            {"provider": "Byte (Dutch hosting)", "confidence": 80},
    "broadview.se":       {"provider": "Broadview (Swedish)", "confidence": 65},
    "routit.net":         {"provider": "Routit (Dutch hosting)", "confidence": 70},
    "antispamcluster.se": {"provider": "AntiSpamCluster (Swedish filter)", "confidence": 65},
    "xefi.fr":            {"provider": "XEFI (French IT services)", "confidence": 70},
    "proxi.technology":   {"provider": "Proxi", "confidence": 55},
    "forss.net":          {"provider": "Forss (Swedish hosting)", "confidence": 65},
    "bhosted.nl":         {"provider": "BHosted (Dutch hosting)", "confidence": 70},
    "easily.co.uk":       {"provider": "Easily (UK hosting)", "confidence": 75},
    "celeonet.fr":        {"provider": "Celeo Networks (French hosting)", "confidence": 70},
    "coaxis.com":         {"provider": "Coaxis (French IT/hosting)", "confidence": 70},
    "glob.cz":            {"provider": "Glob.cz (Czech hosting)", "confidence": 65},
    "tradeindia.com":     {"provider": "TradeIndia (B2B marketplace)", "confidence": 70},
    "mailplug.co.kr":     {"provider": "MailPlug (Korean email)", "confidence": 75},
    "nifcloud.com":       {"provider": "Nifcloud (Fujitsu Japan)", "confidence": 75},
    "dealerspike.com":    {"provider": "DealerSpike (automotive SaaS)", "confidence": 70},
    "brightspace.com":    {"provider": "D2L Brightspace (LMS)", "confidence": 80},
    "ning.com":           {"provider": "Ning (social platform)", "confidence": 70},
    "icodia.com":         {"provider": "Icodia (French hosting)", "confidence": 65},
    "slovanet.net":       {"provider": "Slovanet (Slovak ISP)", "confidence": 75},
    "menufy.com":         {"provider": "Menufy (restaurant ordering)", "confidence": 65},
    "colt-engine.it":     {"provider": "Colt Technology Italy", "confidence": 70},
    "magicmail.fr":       {"provider": "Magic Online (French ISP)", "confidence": 65},
    "magic.fr":           {"provider": "Magic Online (French ISP)", "confidence": 65},
    "adista.fr":          {"provider": "Adista (French telecom)", "confidence": 75},
    "ukrdomen.com":       {"provider": "UkrDomen (Ukrainian hosting)", "confidence": 70},
    "kpnqwest.it":        {"provider": "KPN / GTS Italy (legacy)", "confidence": 65},
    "pp.ua":              {"provider": "PP.UA (Ukrainian hosting)", "confidence": 65},
    "1gb.ua":             {"provider": "1GB.ua (Ukrainian hosting)", "confidence": 65},
    "1gb.com.ua":         {"provider": "1GB.ua (Ukrainian hosting)", "confidence": 65},
    "ukraine.com.ua":     {"provider": "Ukraine.com.ua (hosting)", "confidence": 65},
    "concentric.com":     {"provider": "Concentric (AT&T legacy)", "confidence": 65},
    "bluerange.se":       {"provider": "Bluerange (Swedish)", "confidence": 60},
    "coxmail.com":        {"provider": "Cox Communications", "confidence": 80},
    "worldonline.co.za":  {"provider": "WorldOnline South Africa", "confidence": 65},
    "csloxinfo.com":      {"provider": "CSLoxInfo (Thai ISP)", "confidence": 70},
    "sch.gr":             {"provider": "Greek School Network", "confidence": 70},
    "va.gov":             {"provider": "US Department of Veterans Affairs", "confidence": 85},
    "smtproutes.com":     {"provider": "SMTPRoutes (email relay)", "confidence": 60},
    "smtpbak.com":        {"provider": "SMTPBak (backup MX)", "confidence": 50},
    "savana.cz":          {"provider": "Savana (Czech hosting)", "confidence": 65},
    "glob.cz":            {"provider": "Glob.cz (Czech hosting)", "confidence": 65},
    "domain.not.configured": {"provider": "Misconfigured (no MX)", "confidence": 99},
    "suspended-domain.com":  {"provider": "Suspended domain placeholder", "confidence": 99},
    "0.0.0.0":            {"provider": "Invalid MX record", "confidence": 99},
    "invalid.mx":         {"provider": "Invalid MX placeholder", "confidence": 99},
}

# ---------------------------------------------------------------------------
# Known SPF provider annotations  (include-domain → {provider, type, confidence})
# ---------------------------------------------------------------------------
SPF_KNOWN = {
    "_spf.mailspamprotection.com": {"provider": "N-able SpamExperts", "type": "security", "confidence": 90},
    "spf.dynect.net":              {"provider": "Oracle Dyn (legacy)", "type": "hosting", "confidence": 85},
    "_spf.websupport.sk":          {"provider": "Websupport Slovakia", "type": "hosting", "confidence": 85},
    "spf.webapps.net":             {"provider": "Register.it (team.blue)", "type": "hosting", "confidence": 80},
    "_spf.automattic.com":         {"provider": "Automattic (WordPress.com)", "type": "hosting", "confidence": 90},
    "mail.spf.elkdata.ee":         {"provider": "Elkdata (Estonian hosting)", "type": "hosting", "confidence": 80},
    "_spf.webador.com":            {"provider": "Webador", "type": "hosting", "confidence": 85},
    "_spf.hostedemail.com":        {"provider": "Tucows OpenSRS", "type": "hosting", "confidence": 85},
    "spf.inleed.se":               {"provider": "Inleed (Swedish hosting)", "type": "hosting", "confidence": 85},
    "spf.glesys.se":               {"provider": "GleSYS", "type": "hosting", "confidence": 90},
    "_spf.reliablemail.org":       {"provider": "Reliable Mail", "type": "hosting", "confidence": 65},
    "_spfcls.natrohost.com":       {"provider": "Natrohost", "type": "hosting", "confidence": 65},
    "_netblockshalon.natrohost.com": {"provider": "Natrohost", "type": "hosting", "confidence": 65},
    "_spf.m101.websupport.se":     {"provider": "Websupport Sweden", "type": "hosting", "confidence": 85},
    "spf.flockmail.com":           {"provider": "Titan Email (formerly Flock Mail)", "type": "email_provider", "confidence": 80},
    "spf.stackmail.com":           {"provider": "Stackmail", "type": "hosting", "confidence": 75},
    "reliablemail.org":            {"provider": "Reliable Mail", "type": "hosting", "confidence": 65},
    "spf.surf-town.net":           {"provider": "Surftown", "type": "hosting", "confidence": 85},
    "spf.mijndomeinhosting.nl":    {"provider": "Mijn DomeinHosting (Dutch)", "type": "hosting", "confidence": 75},
    "spfuser.webnode.com":         {"provider": "Webnode", "type": "hosting", "confidence": 85},
    "_spf.webhouse.sk":            {"provider": "Webhouse Slovakia", "type": "hosting", "confidence": 75},
    "emsd1.com":                   {"provider": "Salesforce Marketing Cloud (ExactTarget legacy)", "type": "marketing", "confidence": 80},
    "wanadoo.fr":                  {"provider": "Orange France (Wanadoo legacy)", "type": "hosting", "confidence": 85},
    "spf.mysecurecloudhost.com":   {"provider": "MySecureCloudHost", "type": "hosting", "confidence": 60},
    "spf.host-h.net":              {"provider": "Host-H", "type": "hosting", "confidence": 60},
    "_spf.dnsserver.eu":           {"provider": "DNSServer.eu", "type": "hosting", "confidence": 60},
    "_spf.arandomserver.com":      {"provider": "HawkHost", "type": "hosting", "confidence": 80},
    "spf.webmo.fr":                {"provider": "Webmo (French hosting)", "type": "hosting", "confidence": 70},
    "spf1.mailchannels.net":       {"provider": "MailChannels", "type": "security", "confidence": 90},
    "spf2.mailchannels.net":       {"provider": "MailChannels", "type": "security", "confidence": 90},
    "_spf.fastmail.gr":            {"provider": "FastMail Greece", "type": "email_provider", "confidence": 65},
    "_spf-optimail.linkeo.com":    {"provider": "Linkeo", "type": "hosting", "confidence": 75},
    "_spf.hostcreators.sk":        {"provider": "HostCreators Slovakia", "type": "hosting", "confidence": 70},
    "spf.jabatus.fr":              {"provider": "Jabatus (French hosting)", "type": "hosting", "confidence": 70},
    "agenturserver.de":            {"provider": "Agenturserver (German hosting)", "type": "hosting", "confidence": 70},
    "kundspf.loopia.se":           {"provider": "Loopia", "type": "hosting", "confidence": 90},
    "spf.viaduc.fr":               {"provider": "Viaduc (French hosting)", "type": "hosting", "confidence": 70},
    "ispgateway.de":               {"provider": "ISP Gateway (German email security)", "type": "security", "confidence": 75},
    "spf.marriott.com":            {"provider": "Marriott Hotels", "type": "other", "confidence": 85},
    "_spf.ilait.net":              {"provider": "iLait (Swedish hosting)", "type": "hosting", "confidence": 70},
    "_spf.nameserver.sk":          {"provider": "Nameserver.sk", "type": "hosting", "confidence": 70},
    "_spf.protection.veridyen.com":{"provider": "Veridyen (Turkish hosting)", "type": "hosting", "confidence": 70},
    "filter-out.zxcs.nl":          {"provider": "ZXCS (Dutch hosting)", "type": "hosting", "confidence": 75},
    "justhost.com":                {"provider": "JustHost (Newfold Digital)", "type": "hosting", "confidence": 80},
    "spf.unoeuro.com":             {"provider": "UnoEuro (One.com)", "type": "hosting", "confidence": 85},
    "_spf.webglobe.sk":            {"provider": "Webglobe Slovakia", "type": "hosting", "confidence": 75},
    "spf.whservidor.com":          {"provider": "WH Servidor (Brazilian hosting)", "type": "hosting", "confidence": 70},
    "gridhost.co.uk":              {"provider": "GridHost (UK)", "type": "hosting", "confidence": 70},
    "filters.reliablemail.org":    {"provider": "Reliable Mail", "type": "hosting", "confidence": 65},
    "autodiscover.info":           {"provider": "Autodiscover.info (legacy email discovery)", "type": "hosting", "confidence": 50},
    "gransy.com":                  {"provider": "Gransy (Czech hosting)", "type": "hosting", "confidence": 75},
    "_spf.bizmw.com":              {"provider": "BizMailWorks", "type": "hosting", "confidence": 55},
    "_spf.kmitd.com":              {"provider": "KMITD", "type": "hosting", "confidence": 50},
    "spf.totaalholding.nl":        {"provider": "Totaalholding (Dutch)", "type": "hosting", "confidence": 65},
    "musvc.com":                   {"provider": "Microsoft Universal Service (legacy)", "type": "email_provider", "confidence": 60},
    "_spf.relay.mailprotect.be":   {"provider": "MailProtect Belgium", "type": "security", "confidence": 80},
    "spf.secure.ne.jp":            {"provider": "Secure.ne.jp (Japanese hosting)", "type": "hosting", "confidence": 80},
    "_spf.radicenter.eu":          {"provider": "Radicenter (Italian hosting)", "type": "hosting", "confidence": 75},
    "mxlogin.com":                 {"provider": "MXLogin (email hosting)", "type": "hosting", "confidence": 55},
    "spf.hes.trendmicro.com":      {"provider": "Trend Micro Email Security", "type": "security", "confidence": 90},
    "_spf.zenbox.pl":              {"provider": "Zenbox (Polish hosting)", "type": "hosting", "confidence": 75},
    "spf.cloudus.oxcs.net":        {"provider": "Open-Xchange Cloud (OX App Suite)", "type": "hosting", "confidence": 75},
    "spfv2.proxi.tools":           {"provider": "Proxi", "type": "hosting", "confidence": 55},
    "spf.w4ymail.at":              {"provider": "W4Y (Austrian hosting)", "type": "hosting", "confidence": 65},
    "stspg-customer.com":          {"provider": "Salesforce (transactional email)", "type": "transactional", "confidence": 80},
    "_spf.wy.sk":                  {"provider": "WY.SK (Slovak hosting)", "type": "hosting", "confidence": 70},
    "spf.aserv.co.za":             {"provider": "Aserv (South African hosting)", "type": "hosting", "confidence": 70},
    "sendersrv.com":               {"provider": "SenderSRV (email relay)", "type": "transactional", "confidence": 55},
    "spf.serverdata.net":          {"provider": "ServerData", "type": "hosting", "confidence": 55},
    "namebrightmail.com":          {"provider": "NameBright (domain registrar email)", "type": "hosting", "confidence": 75},
    "spf.online.net":              {"provider": "Online.net / Scaleway (French hosting)", "type": "hosting", "confidence": 80},
    "futurdigital.net":            {"provider": "Futur Digital (French hosting)", "type": "hosting", "confidence": 70},
    "_spf.sui-inter.net":          {"provider": "SUI Internet (French ISP)", "type": "hosting", "confidence": 70},
    "spf.hekko.pl":                {"provider": "Hekko (Polish hosting)", "type": "hosting", "confidence": 75},
    "_spf.ps.kz":                  {"provider": "PS.KZ (Kazakh hosting)", "type": "hosting", "confidence": 75},
    "spf.blacknight.ie":           {"provider": "Blacknight (Irish hosting)", "type": "hosting", "confidence": 85},
    "_spf.site4now.net":           {"provider": "Site4Now", "type": "hosting", "confidence": 65},
    "_mail.glesys.com":            {"provider": "GleSYS", "type": "hosting", "confidence": 90},
    "spf-a.telia.com":             {"provider": "Telia", "type": "hosting", "confidence": 90},
    "spf.easyname.com":            {"provider": "Easyname (Austrian hosting)", "type": "hosting", "confidence": 80},
    "spf.enter-system.com":        {"provider": "Enter System (Russian hosting)", "type": "hosting", "confidence": 70},
    "worldsecuresystems.com":      {"provider": "Adobe Business Catalyst (legacy)", "type": "hosting", "confidence": 80},
    "_spf.yourfilter.nl":          {"provider": "YourFilter (Dutch spam filter)", "type": "security", "confidence": 70},
    "spf.talkactive.net":          {"provider": "TalkActive", "type": "hosting", "confidence": 55},
    "_spf.odoo.com":               {"provider": "Odoo", "type": "erp", "confidence": 90},
    "relay.kinstamailservice.com": {"provider": "Kinsta (WordPress hosting)", "type": "hosting", "confidence": 85},
    "mxlogic.net":                 {"provider": "MXLogic (McAfee, legacy)", "type": "security", "confidence": 80},
    "_spf.acquia.com":             {"provider": "Acquia (Drupal cloud)", "type": "hosting", "confidence": 85},
    "pepipost.net":                {"provider": "Pepipost (transactional email)", "type": "transactional", "confidence": 80},
    "spf.fortnox.se":              {"provider": "Fortnox (Swedish accounting SaaS)", "type": "erp", "confidence": 85},
    "simplelogin.co":              {"provider": "SimpleLogin (email aliasing)", "type": "email_provider", "confidence": 85},
    "spf.tutanota.de":             {"provider": "Tutanota", "type": "email_provider", "confidence": 90},
    "spf.sendinblue.com":          {"provider": "Brevo (Sendinblue)", "type": "marketing", "confidence": 90},  # in case not already there
    "la-boite-immo.fr":            {"provider": "La Boite Immo (French real estate SaaS)", "type": "other", "confidence": 75},
    "sfrbusinessteam.fr":          {"provider": "SFR Business (French telecom)", "type": "hosting", "confidence": 80},
    "antispameurope.com":          {"provider": "AntiSpamEurope", "type": "security", "confidence": 75},
    "verticalresponse.com":        {"provider": "Vertical Response (email marketing)", "type": "marketing", "confidence": 80},
    "emarsys.net":                 {"provider": "Emarsys (SAP)", "type": "marketing", "confidence": 85},
    "ccsend.com":                  {"provider": "Constant Contact", "type": "marketing", "confidence": 85},
    "netcore.co.in":               {"provider": "Netcore Cloud (Indian transactional)", "type": "transactional", "confidence": 80},
    "spf.sendcloud.org":           {"provider": "Sendcloud (logistics email)", "type": "transactional", "confidence": 75},
    "mmsend.com":                  {"provider": "Silverpop / IBM Watson Marketing (legacy)", "type": "marketing", "confidence": 75},
    "datadrivenemail.com":         {"provider": "Data Driven Email", "type": "marketing", "confidence": 55},
    "getanewsletter.com":          {"provider": "Get a Newsletter", "type": "marketing", "confidence": 70},
    "mailingboss.net":             {"provider": "MailingBoss", "type": "marketing", "confidence": 65},
    "spf.hiworks.co.kr":           {"provider": "Hiworks (Korean business email)", "type": "email_provider", "confidence": 80},
    "mailplug.com":                {"provider": "MailPlug (Korean email)", "type": "email_provider", "confidence": 75},
    "spf.dotmailer.com":           {"provider": "Dotdigital", "type": "marketing", "confidence": 85},
    "spf.mailigen.com":            {"provider": "Mailigen (email marketing)", "type": "marketing", "confidence": 75},
    "spf.usa.net":                 {"provider": "USA.net (legacy email hosting)", "type": "email_provider", "confidence": 70},
    "spf.bluetie.com":             {"provider": "BlueTie (hosted Exchange)", "type": "email_provider", "confidence": 70},
    "spf.postini.com":             {"provider": "Postini / Google (legacy filter)", "type": "security", "confidence": 85},
    "spf.mailroute.net":           {"provider": "MailRoute (email security)", "type": "security", "confidence": 80},
    "spf.synxis.com":              {"provider": "SynXis / Sabre (hospitality)", "type": "other", "confidence": 75},
    "spf.listrak.com":             {"provider": "Listrak (retail email marketing)", "type": "marketing", "confidence": 80},
    "wildapricot.org":             {"provider": "Wild Apricot (Personify)", "type": "other", "confidence": 80},
    "spf.memberzone.com":          {"provider": "MemberZone (membership management)", "type": "other", "confidence": 65},
    "spf.campminder.com":          {"provider": "CampMinder (camp management)", "type": "other", "confidence": 70},
    "spf.jobdiva.com":             {"provider": "JobDiva (ATS/CRM)", "type": "crm", "confidence": 75},
    "spf.tapfiliate.com":          {"provider": "Tapfiliate (affiliate marketing)", "type": "marketing", "confidence": 75},
    "spf.dealerspike.com":         {"provider": "DealerSpike (automotive)", "type": "other", "confidence": 70},
    "spf.kinepolis.com":           {"provider": "Kinepolis (cinema chain)", "type": "other", "confidence": 75},
    "spf.slb.com":                 {"provider": "SLB (Schlumberger)", "type": "other", "confidence": 80},
    "spf.smarsh.com":              {"provider": "Smarsh (compliance)", "type": "other", "confidence": 75},
    "spf.ouvaton.coop":            {"provider": "Ouvaton (cooperative hosting)", "type": "hosting", "confidence": 75},
    "spf.greenhost.nl":            {"provider": "Greenhost (Dutch green hosting)", "type": "hosting", "confidence": 75},
    "spf.umbler.com":              {"provider": "Umbler (Brazilian hosting)", "type": "hosting", "confidence": 75},
    "spf.vevida.com":              {"provider": "Vevida (Dutch hosting)", "type": "hosting", "confidence": 75},
    "spf.dandomain.dk":            {"provider": "DanDomain (Danish hosting)", "type": "hosting", "confidence": 80},
    "spf.curanet.dk":              {"provider": "Curanet (Danish hosting)", "type": "hosting", "confidence": 75},
    "spf.simply.com":              {"provider": "Simply.com", "type": "hosting", "confidence": 85},
    "spf.splio.com":               {"provider": "Splio (European marketing platform)", "type": "marketing", "confidence": 75},
    "spf.ubivox.com":              {"provider": "Ubivox (Danish email marketing)", "type": "marketing", "confidence": 75},
    "spf.raiolanetworks.com":      {"provider": "Raiola Networks (Spanish hosting)", "type": "hosting", "confidence": 75},
    "spf.lcn.com":                 {"provider": "LCN.com (UK hosting)", "type": "hosting", "confidence": 75},
    "spf.webhostingireland.ie":    {"provider": "WebHosting Ireland", "type": "hosting", "confidence": 75},
    "spf.phpnet.org":              {"provider": "PHPnet (French hosting)", "type": "hosting", "confidence": 75},
    "spf.variomedia.de":           {"provider": "Variomedia (German hosting)", "type": "hosting", "confidence": 75},
    "spf.abicart.com":             {"provider": "Abicart (Swedish ecommerce)", "type": "ecommerce", "confidence": 75},
    "spf.internet.se":             {"provider": "Internet.se", "type": "hosting", "confidence": 70},
    "spf.crystone.se":             {"provider": "Crystone", "type": "hosting", "confidence": 80},
    "spf.ballou.se":               {"provider": "Ballou", "type": "hosting", "confidence": 75},
    "spf.egensajt.se":             {"provider": "Egensajt", "type": "hosting", "confidence": 75},
    "_spf.egensajt.se":            {"provider": "Egensajt", "type": "hosting", "confidence": 75},
    "spf.inleed.se":               {"provider": "Inleed", "type": "hosting", "confidence": 85},
    "spf.loopia.se":               {"provider": "Loopia", "type": "hosting", "confidence": 90},
    "spf.wopsa.se":                {"provider": "Wopsa (Swedish hosting)", "type": "hosting", "confidence": 70},
    "wopsa.se":                    {"provider": "Wopsa (Swedish hosting)", "type": "hosting", "confidence": 70},
    "spf.inviso.se":               {"provider": "Inviso (Swedish)", "type": "hosting", "confidence": 65},
    "spf.websupport.se":           {"provider": "Websupport Sweden", "type": "hosting", "confidence": 85},
    "spf.svenskadomaner.se":       {"provider": "Svenska Domäner", "type": "hosting", "confidence": 75},
    "spf.glesys.se":               {"provider": "GleSYS", "type": "hosting", "confidence": 90},
    "spf.inleed.se":               {"provider": "Inleed", "type": "hosting", "confidence": 85},
    "_spf.zone.eu":                {"provider": "Zone Media", "type": "hosting", "confidence": 80},
    "_spf.livemail.co.uk":         {"provider": "Livemail (UK hosting)", "type": "hosting", "confidence": 65},
    "_spf.manitu.net":             {"provider": "Manitu (German hosting)", "type": "hosting", "confidence": 75},
    "_spf.freshbooks.com":         {"provider": "FreshBooks (accounting SaaS)", "type": "erp", "confidence": 85},
    "spf.forss.net":               {"provider": "Forss (Swedish hosting)", "type": "hosting", "confidence": 65},
    "spf.byte.nl":                 {"provider": "Byte (Dutch hosting)", "type": "hosting", "confidence": 80},
    "_spf.webglobe.cz":            {"provider": "Webglobe Czech", "type": "hosting", "confidence": 75},
    "spf.mxserver.ro":             {"provider": "MXServer (Romanian hosting)", "type": "hosting", "confidence": 70},
    "_spf.exactonline.nl":         {"provider": "Exact Online (Dutch ERP)", "type": "erp", "confidence": 85},
    "spf.spamservice.nl":          {"provider": "SpamService (Dutch filter)", "type": "security", "confidence": 70},
    "_spf.emaillabs.net.pl":       {"provider": "EmailLabs (Polish email platform)", "type": "transactional", "confidence": 75},
    "spf.icoremail.net":           {"provider": "iCoreMail (Chinese email)", "type": "email_provider", "confidence": 75},
    "spf.wootemple.com":           {"provider": "WooTemple", "type": "ecommerce", "confidence": 55},
    "_spf.smtp.mailtrap.live":     {"provider": "Mailtrap", "type": "transactional", "confidence": 85},
    "spf.umbler.com":              {"provider": "Umbler (Brazil)", "type": "hosting", "confidence": 75},
    "spf.tutanota.de":             {"provider": "Tutanota", "type": "email_provider", "confidence": 90},
    "simplelogin.co":              {"provider": "SimpleLogin", "type": "email_provider", "confidence": 85},
    "mailo.com":                   {"provider": "Mailo (French email)", "type": "email_provider", "confidence": 80},
    "spf.oximailing.com":          {"provider": "OXImailing (French email marketing)", "type": "marketing", "confidence": 70},
    "oubound.mailhop.org":         {"provider": "MailHop (Dyn/Oracle legacy)", "type": "security", "confidence": 70},
    "outbound.mailhop.org":        {"provider": "MailHop (Dyn/Oracle legacy)", "type": "security", "confidence": 70},
    "_spf.bookmyname.com":         {"provider": "BookMyName (French registrar)", "type": "hosting", "confidence": 75},
    "spf.raiolanetworks.com":      {"provider": "Raiola Networks (Spanish hosting)", "type": "hosting", "confidence": 75},
    "dnsexit.com":                 {"provider": "DNSExit (dynamic DNS/email)", "type": "hosting", "confidence": 70},
    "spf.protection.3dcart.com":   {"provider": "3dcart (now Shift4Shop, ecommerce)", "type": "ecommerce", "confidence": 80},
    "_spf.exactonline.nl":         {"provider": "Exact Online", "type": "erp", "confidence": 85},
    "spf.netsolmail.net":          {"provider": "Network Solutions email", "type": "hosting", "confidence": 75},
    "spf.everycloudtech.com":      {"provider": "EveryCloud (email security)", "type": "security", "confidence": 70},
    "spf.routit.net":              {"provider": "Routit (Dutch hosting)", "type": "hosting", "confidence": 70},
    "spf.haisoft.net":             {"provider": "Haisoft (French hosting)", "type": "hosting", "confidence": 70},
    "spf.phpnet.org":              {"provider": "PHPnet (French hosting)", "type": "hosting", "confidence": 75},
    "spf.kalanda.net":             {"provider": "Kalanda (hosting)", "type": "hosting", "confidence": 55},
    "spf.arobiz.pro":              {"provider": "AroBiz", "type": "hosting", "confidence": 50},
    "spf.jchost.pl":               {"provider": "JCHost (Polish hosting)", "type": "hosting", "confidence": 65},
    "spf.webhuset.no":             {"provider": "Webhuset (Norwegian hosting)", "type": "hosting", "confidence": 75},
    "_spf.proisp.no":              {"provider": "ProISP (Norwegian hosting)", "type": "hosting", "confidence": 75},
    "spf.reg365.net":              {"provider": "Reg365 (UK hosting)", "type": "hosting", "confidence": 70},
    "spf.protection.cyon.net":     {"provider": "Cyon (Swiss hosting)", "type": "hosting", "confidence": 80},
    "spf.mail.webland.ch":         {"provider": "Webland (Swiss hosting)", "type": "hosting", "confidence": 75},
    "spf.switchplus-mail.ch":      {"provider": "Switchplus (Swiss hosting)", "type": "hosting", "confidence": 75},
    "spf.zoner.fi":                {"provider": "Zoner (Finnish hosting)", "type": "hosting", "confidence": 75},
    "_spf.inet.sk":                {"provider": "iNET Slovakia", "type": "hosting", "confidence": 70},
    "_spf.nichosting.sk":          {"provider": "NIC Hosting Slovakia", "type": "hosting", "confidence": 70},
    "_spf.speedweb.sk":            {"provider": "Speedweb Slovakia", "type": "hosting", "confidence": 70},
    "_spf.webygroup.sk":           {"provider": "WebyGroup Slovakia", "type": "hosting", "confidence": 70},
    "_spf.eshop-rychle.cz":        {"provider": "Eshop-Rychle (Czech ecommerce)", "type": "ecommerce", "confidence": 75},
    "_spf.eshop-rychlo.sk":        {"provider": "Eshop-Rychlo (Slovak ecommerce)", "type": "ecommerce", "confidence": 75},
    "spf.blueboard.cz":            {"provider": "Blueboard (Czech hosting)", "type": "hosting", "confidence": 70},
    "smtp-gw.gigaserver.cz":       {"provider": "GigaServer (Czech hosting)", "type": "hosting", "confidence": 70},
    "spf.cesky-hosting.cz":        {"provider": "Český Hosting (Czech)", "type": "hosting", "confidence": 70},
    "_spf.ignum.cz":               {"provider": "Ignum (Czech hosting)", "type": "hosting", "confidence": 75},
    "_spf.we.wedos.net":           {"provider": "Wedos (Czech hosting)", "type": "hosting", "confidence": 75},
    "_spf.websupport.cz":          {"provider": "Websupport Czech", "type": "hosting", "confidence": 80},
    "spf.tld-mx.com":              {"provider": "TLD-MX (email routing)", "type": "hosting", "confidence": 55},
    "spf.cn4e.com":                {"provider": "CN4E (Chinese hosting)", "type": "hosting", "confidence": 60},
    "spf.263.net":                 {"provider": "263.net (Chinese email)", "type": "email_provider", "confidence": 75},
    "spf.263xmail.com":            {"provider": "263 Enterprise Email (China)", "type": "email_provider", "confidence": 75},
    "spf.icoremail.net":           {"provider": "iCoreMail (Chinese)", "type": "email_provider", "confidence": 75},
    "spf.zmail300.cn":             {"provider": "ZMail (Chinese email)", "type": "email_provider", "confidence": 65},
    "spf.global-mail.cn":          {"provider": "Global Mail China", "type": "email_provider", "confidence": 65},
    "emailserver.vn":              {"provider": "EmailServer.vn (Vietnamese)", "type": "hosting", "confidence": 65},
    "hibox.hinet.net":             {"provider": "HiNet (Chunghwa Telecom Taiwan)", "type": "hosting", "confidence": 80},
    "spf.listrak.com":             {"provider": "Listrak (retail marketing)", "type": "marketing", "confidence": 80},
    "constantcontact.com":         {"provider": "Constant Contact", "type": "marketing", "confidence": 90},
    "spf.protection.markum.net":   {"provider": "Markum (email security)", "type": "security", "confidence": 65},
    "includespf.security-mail.net":{"provider": "Security-Mail.net", "type": "security", "confidence": 60},
    "spf.aams4.jp":                {"provider": "AAMS Japan (Japanese hosting)", "type": "hosting", "confidence": 65},
    "spf.aams6.jp":                {"provider": "AAMS Japan (Japanese hosting)", "type": "hosting", "confidence": 65},
    "spf.sender.xserver.jp":       {"provider": "Xserver Japan", "type": "hosting", "confidence": 80},
    "spf.alpha-prm.jp":            {"provider": "Alpha PRM (Japanese hosting)", "type": "hosting", "confidence": 65},
    "spf.repica.jp":               {"provider": "Repica (Japanese)", "type": "hosting", "confidence": 60},
    "spf.futoka.jp":               {"provider": "Futoka (Japanese hosting)", "type": "hosting", "confidence": 60},
    "spf.shopserve.jp":            {"provider": "ShopServe (Japanese ecommerce)", "type": "ecommerce", "confidence": 70},
    "spf.sender.netowl.jp":        {"provider": "NetOwl (Japanese hosting)", "type": "hosting", "confidence": 65},
    "spf.gmoserver.jp":            {"provider": "GMO Server (Japanese hosting)", "type": "hosting", "confidence": 80},
    "spf.bmv.jp":                  {"provider": "BMV Japan", "type": "hosting", "confidence": 55},
    "spf.haihaimail.jp":           {"provider": "Haihai Mail (Japanese)", "type": "email_provider", "confidence": 60},
    "fmx.etius.jp":                {"provider": "Etius (Japanese hosting)", "type": "hosting", "confidence": 60},
    "spfgw.fsi.ne.jp":             {"provider": "FSI (Japanese ISP)", "type": "hosting", "confidence": 65},
    "myasp.jp":                    {"provider": "MyASP (Japanese email marketing)", "type": "marketing", "confidence": 70},
    "mxr.valueserver.jp":          {"provider": "ValueServer (Japanese hosting)", "type": "hosting", "confidence": 70},
    "xaas3.jp":                    {"provider": "XaaS Japan", "type": "hosting", "confidence": 55},
    "spf.q-send.jp":               {"provider": "Q-Send (Japanese email)", "type": "transactional", "confidence": 60},
    "nc2.nicmail.ru":              {"provider": "NicMail (Russian hosting)", "type": "hosting", "confidence": 65},
    "dc1.nicmail.ru":              {"provider": "NicMail (Russian hosting)", "type": "hosting", "confidence": 65},
    "dc2.nicmail.ru":              {"provider": "NicMail (Russian hosting)", "type": "hosting", "confidence": 65},
    "_spf.majordomo.ru":           {"provider": "Majordomo (Russian hosting)", "type": "hosting", "confidence": 75},
    "netangels.ru":                {"provider": "NetAngels (Russian hosting)", "type": "hosting", "confidence": 70},
    "mail.insales.ru":             {"provider": "InSales (Russian ecommerce)", "type": "ecommerce", "confidence": 75},
    "setup.ru":                    {"provider": "Setup.ru (Russian hosting)", "type": "hosting", "confidence": 70},
    "getcourse.ru":                {"provider": "GetCourse (Russian LMS)", "type": "other", "confidence": 75},
    "send-box.ru":                 {"provider": "Send-Box (Russian email)", "type": "transactional", "confidence": 65},
    "_spf.hosting-srv.net":        {"provider": "HostingSRV", "type": "hosting", "confidence": 55},
    "relay.romarg.net":            {"provider": "RomArg (Romanian hosting)", "type": "hosting", "confidence": 65},
    "relay.dc.besthosting.ua":     {"provider": "BestHosting Ukraine", "type": "hosting", "confidence": 65},
    "relay.vhosting-it.com":       {"provider": "VHosting Italy", "type": "hosting", "confidence": 65},
    "hostedoffice.ag":             {"provider": "HostedOffice (hosted Exchange)", "type": "email_provider", "confidence": 65},
    "spfa.alinto.net":             {"provider": "Alinto (French email security)", "type": "security", "confidence": 75},
    "spf.cftech.com":              {"provider": "CF Tech", "type": "hosting", "confidence": 50},
    "spf.pulseheberg.com":         {"provider": "PulseHeberg (French hosting)", "type": "hosting", "confidence": 70},
    "no-ip.com":                   {"provider": "No-IP (dynamic DNS)", "type": "hosting", "confidence": 75},
    "spf.messaging.microsoft.com": {"provider": "Microsoft (legacy outbound)", "type": "email_provider", "confidence": 85},
    "spf.microsoftonline.com":     {"provider": "Microsoft 365 (legacy SPF)", "type": "email_provider", "confidence": 90},
    "spf.listrak.com":             {"provider": "Listrak", "type": "marketing", "confidence": 80},
    "spf.usa.net":                 {"provider": "USA.net (legacy)", "type": "email_provider", "confidence": 70},
    "tigertech.net":               {"provider": "Tiger Technologies", "type": "hosting", "confidence": 70},
    "learnybox.com":               {"provider": "LearnYBox (French LMS)", "type": "other", "confidence": 70},
    "spf.skymail.net.br":          {"provider": "Skymail (Brazilian email)", "type": "hosting", "confidence": 65},
    "spf.mandic.com.br":           {"provider": "Mandic (Brazil)", "type": "hosting", "confidence": 75},
    "spf.redehost.com.br":         {"provider": "RedeHost (Brazil)", "type": "hosting", "confidence": 75},
    "spf.umbler.com":              {"provider": "Umbler (Brazil)", "type": "hosting", "confidence": 75},
    "_spf.locaweb.com.br":         {"provider": "Locaweb (Brazilian hosting)", "type": "hosting", "confidence": 85},
    "_spf.tray.com.br":            {"provider": "Tray (Brazilian ecommerce)", "type": "ecommerce", "confidence": 75},
    "spf1.auinmeio.com.br":        {"provider": "Au In Meio (Brazilian hosting)", "type": "hosting", "confidence": 60},
    "spf2.auinmeio.com.br":        {"provider": "Au In Meio (Brazilian hosting)", "type": "hosting", "confidence": 60},
    "_spf.emailmkt.correio.ws":    {"provider": "Correio.ws (Brazilian email)", "type": "marketing", "confidence": 65},
    "aruba.it":                    {"provider": "Aruba (Italian hosting)", "type": "hosting", "confidence": 90},
    "_spf.armada.it":              {"provider": "Armada (Italian hosting)", "type": "hosting", "confidence": 65},
    "_spf.th.seeweb.it":           {"provider": "Seeweb (Italian hosting)", "type": "hosting", "confidence": 75},
    "_spf.radicenter.eu":          {"provider": "Radicenter (Italian)", "type": "hosting", "confidence": 75},
    "_spf.cretaforce.gr":          {"provider": "CretaForce (Greek hosting)", "type": "hosting", "confidence": 65},
    "spf.scannet.dk":              {"provider": "Scannet (Danish hosting)", "type": "hosting", "confidence": 75},
    "spf.powerhosting.dk":         {"provider": "Powerhosting Denmark", "type": "hosting", "confidence": 75},
    "_spf.powerhosting.dk":        {"provider": "Powerhosting Denmark", "type": "hosting", "confidence": 75},
    "spf.mijn.host":               {"provider": "Mijn.host (Dutch hosting)", "type": "hosting", "confidence": 75},
    "_spf.argewebhosting.nl":      {"provider": "ArgeWebHosting (Dutch)", "type": "hosting", "confidence": 65},
    "spf.ixlhosting.nl":           {"provider": "IXL Hosting (Dutch)", "type": "hosting", "confidence": 70},
    "_spf4.pcextreme.nl":          {"provider": "PCExtreme (Dutch hosting)", "type": "hosting", "confidence": 75},
    "_spf6.pcextreme.nl":          {"provider": "PCExtreme (Dutch hosting)", "type": "hosting", "confidence": 75},
    "mailfilter.myrootnet.nl":     {"provider": "MyRootNet (Dutch)", "type": "security", "confidence": 60},
    "_spf.zorgmail.nl":            {"provider": "Zorgmail (Dutch healthcare email)", "type": "email_provider", "confidence": 75},
    "_spf.mailplus.nl":            {"provider": "MailPlus (Dutch email)", "type": "email_provider", "confidence": 75},
    "spf.mijnwebwinkel.nl":        {"provider": "MijnWebwinkel (Dutch ecommerce)", "type": "ecommerce", "confidence": 75},
    "_spf.afasonline.nl":          {"provider": "AFA Online (Dutch)", "type": "hosting", "confidence": 60},
    "_spf.jouwweb.nl":             {"provider": "JouwWeb (Dutch website builder)", "type": "hosting", "confidence": 80},
    "_spf2.trwww.com":             {"provider": "TRWWW", "type": "hosting", "confidence": 50},
    "_spf.trwww.com":              {"provider": "TRWWW", "type": "hosting", "confidence": 50},
    "spf.domainoo.fr":             {"provider": "Domainoo (French hosting)", "type": "hosting", "confidence": 70},
    "_spf-optimail.linkeo.com":    {"provider": "Linkeo", "type": "marketing", "confidence": 75},
    "_spf.orange-business.fr":     {"provider": "Orange Business Services", "type": "hosting", "confidence": 85},
    "spf.online.net":              {"provider": "Online.net / Scaleway", "type": "hosting", "confidence": 80},
    "spf.gpaas.net":               {"provider": "Gandi PaaS (Gandi hosting)", "type": "hosting", "confidence": 75},
    "spf.advango.fr":              {"provider": "Advango (French hosting)", "type": "hosting", "confidence": 60},
    "spf.ics.fr":                  {"provider": "ICS France", "type": "hosting", "confidence": 60},
    "spf2.ics.fr":                 {"provider": "ICS France", "type": "hosting", "confidence": 60},
    "spf.immo-facile.com":         {"provider": "Immo-Facile (French real estate)", "type": "other", "confidence": 65},
    "spf.viaduc.fr":               {"provider": "Viaduc (French hosting)", "type": "hosting", "confidence": 70},
    "spf.webmo.fr":                {"provider": "Webmo (French hosting)", "type": "hosting", "confidence": 70},
    "spf.sitew.com":               {"provider": "SiteW (French website builder)", "type": "hosting", "confidence": 80},
    "_spf.gpaas.net":              {"provider": "Gandi PaaS", "type": "hosting", "confidence": 75},
    "spf.cloudus.rs.oxcs.net":     {"provider": "Open-Xchange (OX App Suite)", "type": "hosting", "confidence": 70},
    "spf.cloudeu.xion.oxcs.net":   {"provider": "Open-Xchange (OX App Suite)", "type": "hosting", "confidence": 70},
    "eig.spf.a.cloudfilter.net":   {"provider": "EIG / Newfold Digital (spam filter)", "type": "security", "confidence": 75},
    "spf.azehosting.net":          {"provider": "AZE Hosting", "type": "hosting", "confidence": 55},
    "spf.biznes-host.pl":          {"provider": "Biznes-Host (Polish hosting)", "type": "hosting", "confidence": 65},
    "spf.agnat.pl":                {"provider": "Agnat (Polish hosting)", "type": "hosting", "confidence": 65},
    "_spfw.superhost.pl":          {"provider": "Super-Host.pl", "type": "hosting", "confidence": 75},
    "_spfw2.superhost.pl":         {"provider": "Super-Host.pl", "type": "hosting", "confidence": 75},
    "_spf.atthost.pl":             {"provider": "ATT Host (Polish hosting)", "type": "hosting", "confidence": 65},
    "_spf.webd.pl":                {"provider": "WebD (Polish hosting)", "type": "hosting", "confidence": 65},
    "spf.host.it":                 {"provider": "Host.it (Italian hosting)", "type": "hosting", "confidence": 75},
    "_spf.ergonet.it":             {"provider": "ErgoNet (Italian hosting)", "type": "hosting", "confidence": 65},
    "gandi.net":                   {"provider": "Gandi", "type": "hosting", "confidence": 85},
    "spf.protection.outlook.com":  {"provider": "Microsoft 365", "type": "email_provider", "confidence": 99},
    "protection.outlook.com":      {"provider": "Microsoft 365", "type": "email_provider", "confidence": 99},
    "spf.dynect.net":              {"provider": "Oracle Dyn (legacy)", "type": "hosting", "confidence": 85},
    "googlemail.com":              {"provider": "Google Workspace (legacy domain)", "type": "email_provider", "confidence": 95},
    "etsy.com":                    {"provider": "Etsy (marketplace)", "type": "ecommerce", "confidence": 85},
    "amazon.com":                  {"provider": "Amazon (Amazon SES / corporate)", "type": "transactional", "confidence": 80},
    "amazonaws.com":               {"provider": "Amazon SES", "type": "transactional", "confidence": 80},
    "unilever.com":                {"provider": "Unilever (corporate)", "type": "other", "confidence": 85},
    "va.gov":                      {"provider": "US Dept of Veterans Affairs", "type": "other", "confidence": 85},
    "spf.salesforce.com":          {"provider": "Salesforce", "type": "crm", "confidence": 90},
    "smtp.zendesk.com":            {"provider": "Zendesk", "type": "helpdesk", "confidence": 90},
    "dispatch-us.ppe-hosted.com":  {"provider": "Proofpoint Essentials", "type": "security", "confidence": 80},
    "spf.sendingservice.net":      {"provider": "SendingService", "type": "transactional", "confidence": 55},
    "spf.mailengine1.com":         {"provider": "MailEngine", "type": "transactional", "confidence": 50},
    "spf.shared.spaceship.host":   {"provider": "Spaceship (Namecheap)", "type": "hosting", "confidence": 75},
    "_spf.1stdomains.co.nz":       {"provider": "1st Domains (NZ hosting)", "type": "hosting", "confidence": 70},
    "_spf.hosts.net.nz":           {"provider": "Hosts.net.nz (NZ hosting)", "type": "hosting", "confidence": 70},
    "spf.nz.smxemail.com":         {"provider": "SMX Email (NZ email security)", "type": "security", "confidence": 75},
    "spf.mailrouteapp.com":        {"provider": "MailRoute (email security)", "type": "security", "confidence": 75},
}


# ---------------------------------------------------------------------------
# Report parser
# ---------------------------------------------------------------------------

def extract_from_report(filepath: Path, section_marker: str) -> dict[str, int]:
    results = {}
    in_section = False
    for line in open(filepath):
        if section_marker in line:
            in_section = True
            continue
        if in_section:
            s = line.strip()
            if not s or '===' in s or 'Building MX' in s:
                continue
            if ('rank' in s.lower() and 'domains' in s.lower()) or s.startswith('---'):
                continue
            if 'entries shown' in s or 'Total time' in s:
                break
            parts = s.split()
            if len(parts) >= 3 and parts[0].isdigit():
                try:
                    count = int(parts[-2].replace(',', ''))
                    results[parts[1]] = count
                except Exception:
                    pass
    return results


def clean_domain(d: str) -> str:
    """Derive a readable provider name from an unknown domain."""
    d = d.lstrip('_').lstrip('.')
    for prefix in ('spf.', 'spf1.', 'spf2.', 'mail.spf.', 'mail.', 'smtp.', 'relay.'):
        if d.startswith(prefix):
            d = d[len(prefix):]
            break
    # strip known suffixes
    for suffix in ('.com', '.net', '.org', '.io', '.co', '.eu', '.fr', '.de',
                   '.nl', '.se', '.sk', '.cz', '.pl', '.ru', '.ua', '.br',
                   '.jp', '.au', '.ca', '.it', '.es', '.be', '.ch', '.at',
                   '.dk', '.no', '.fi', '.gr', '.bg', '.ro', '.hu', '.ee', '.lt'):
        if d.endswith(suffix):
            d = d[:-len(suffix)]
            break
    return d.replace('-', ' ').replace('.', ' ').title()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # Load existing tables
    mx_existing  = json.loads(MX_TABLE.read_text())
    spf_existing = json.loads(SPF_TABLE.read_text())

    # Extract historical candidates
    mx_hist  = extract_from_report(MX_HIST_REPORT,  'TOP MX PROVIDERS')
    spf_hist = extract_from_report(SPF_HIST_REPORT, 'TOP include: TARGETS')

    # Build MX candidates
    mx_added = {}
    for domain, count in sorted(mx_hist.items(), key=lambda x: -x[1]):
        if count < THRESHOLD:
            continue
        if domain in mx_existing:
            continue
        if domain in MX_KNOWN:
            entry = MX_KNOWN[domain]
        else:
            entry = {"provider": clean_domain(domain), "confidence": 45}
        mx_added[domain] = entry

    # Build SPF candidates
    spf_added = {}
    for domain, count in sorted(spf_hist.items(), key=lambda x: -x[1]):
        if count < THRESHOLD:
            continue
        if domain in spf_existing:
            continue
        if domain in SPF_KNOWN:
            entry = SPF_KNOWN[domain]
        else:
            entry = {"provider": clean_domain(domain), "type": "hosting", "confidence": 45}
        spf_added[domain] = entry

    # Stats
    print(f"MX:  {len(mx_existing)} existing  +  {len(mx_added)} new  →  {len(mx_existing)+len(mx_added)} total")
    print(f"SPF: {len(spf_existing)} existing  +  {len(spf_added)} new  →  {len(spf_existing)+len(spf_added)} total")
    print(f"\nTop 20 MX additions:")
    for i, (d, e) in enumerate(list(mx_added.items())[:20]):
        print(f"  {mx_hist[d]:>8,}  {d:<35}  → {e['provider']} (conf:{e['confidence']})")
    print(f"\nTop 20 SPF additions:")
    for i, (d, e) in enumerate(list(spf_added.items())[:20]):
        print(f"  {spf_hist[d]:>8,}  {d:<40}  → {e['provider']} (conf:{e['confidence']})")

    if args.dry_run:
        print("\n[dry-run] no files written")
        return

    # Merge: existing wins
    mx_merged  = {**mx_added,  **mx_existing}
    spf_merged = {**spf_added, **spf_existing}

    MX_TABLE.write_text(json.dumps(mx_merged,  indent=2, ensure_ascii=False) + '\n')
    SPF_TABLE.write_text(json.dumps(spf_merged, indent=2, ensure_ascii=False) + '\n')
    print(f"\nWritten: {MX_TABLE}")
    print(f"Written: {SPF_TABLE}")


if __name__ == '__main__':
    main()
