from __future__ import annotations

"""Explicit query inventories and source mappings ported from the original retrieval notebooks.

Source material used once during reconstruction:
- NID_Retrieval_Pipeline_EURLEX.ipynb
- NID_Retrieval_Pipeline_EURLEX_NIM.ipynb
- NID_Retrieval_Pipeline_non_EU_countries.ipynb

These constants are now runtime-independent of those notebooks.

Important fidelity note for the EUR-Lex layer:
- `EU_EURLEX_SEARCH_TERMS_PRIMARY` matches the original primary WebService
  search list from `NID_Retrieval_Pipeline_EURLEX.ipynb`.
- `net-gain` / `net gain` were *not* in that primary search list. They appear
  in the later body/full-text matching layers and remain there intentionally.
"""

SEARCH_TERMS_PRIMARY = ['nature-positive', 'nature positive', 'nature inclusive', 'nature-inclusive', 'nature-inclusive', 'nature inclusive design','nature based solutions', 'nature-based solutions', 'biodiversity net gain', 'biodiversity net-gain', 'biodiversity gain', 'nature restoration', 'nature repair', 'biodiversity strategy']
TRANSLATED_TERMS_PRIMARY = {
  "bg": [
    # nature-inclusive / nature-inclusive design (kept)
    "интегриран с природата дизайн",
    "природосъобразен дизайн",
    # nature-positive (kept)
    "природопозитивен",
    # nature-based solutions (verified)
    "природосъобразни решения",
    # biodiversity strategy (kept)
    "стратегия за биологичното разнообразие",
    "стратегия на ЕС за биологичното разнообразие",
    # nature restoration (suggested supplement; EU texts also use “възстановяване” wording)
    "възстановяване на природата",
    "възстановяване на екосистемите",
  ],

  "cs": [
    "návrh zahrnující přírodu",
    "pozitivní pro přírodu",
    # nature-based solutions (verified)
    "řešení založená na přírodě",
    # biodiversity strategy
    "strategie v oblasti biologické rozmanitosti",
    "strategie EU v oblasti biologické rozmanitosti",
    # nature restoration (suggested; EU also uses obnova přírody in nature restoration context)
    "obnova přírody",
    "obnova ekosystémů",
  ],

  "da": [
    "naturinkluderende",
    "naturinkluderende design",
    "naturpositiv",
    # nature-based solutions (verified)
    "naturbaserede løsninger",
    # biodiversity strategy
    "biodiversitetsstrategi",
    "EU's biodiversitetsstrategi",
    # nature restoration (verified/suggested)
    "genopretning af natur",
    "genopretning af biodiversitet",
  ],

  "de": [
    "naturverträgliche Planung",
    "naturpositiv",
    # nature-based solutions (verified)
    "naturbasierte Lösungen",
    "naturbasierte Lösung",
    # net gain (verified in EU biodiversity strategy wording)
    "Netto-Gewinn",
    "Netto-Gewinn für die Biodiversität",
    # biodiversity strategy
    "Biodiversitätsstrategie",
    "EU-Biodiversitätsstrategie",
    # nature restoration (verified/suggested)
    "Wiederherstellung der Natur",
    "Wiederherstellung von Ökosystemen",
  ],

  "el": [
    "φιλικός προς τη φύση σχεδιασμός",
    "θετικός για τη φύση",
    # nature-based solutions (verified)
    "λύσεις βασισμένες στη φύση",
    # net gain (kept, but more biodiversity-specific likely improves precision)
    "καθαρό περιβαλλοντικό κέρδος",
    "καθαρό κέρδος για τη βιοποικιλότητα",
    # biodiversity strategy
    "στρατηγική για τη βιοποικιλότητα",
    "στρατηγική της ΕΕ για τη βιοποικιλότητα",
    # nature restoration (suggested)
    "αποκατάσταση της φύσης",
    "αποκατάσταση οικοσυστημάτων",
  ],

  "es": [
    "diseño respetuoso con la naturaleza",
    "positivo para la naturaleza",
    # nature-based solutions (verified)
    "soluciones basadas en la naturaleza",
    # net gain (aligned to EU strategy language)
    "ganancia neta",
    "ganancia neta en biodiversidad",
    "ganancia neta para la biodiversidad",
    # biodiversity strategy
    "estrategia de biodiversidad",
    "estrategia de la UE sobre la biodiversidad",
    # nature restoration (EU texts use “recuperación”; regulation context often uses “restauración”)
    "recuperación de la naturaleza",
    "restauración de la naturaleza",
    "restauración de los ecosistemas",
  ],

  "et": [
    "loodusega arvestav disain",
    "looduspositiivne",
    # nature-based solutions (verified)
    "looduspõhised lahendused",
    "looduspõhine lahendus",
    # biodiversity strategy
    "elurikkuse strateegia",
    "ELi elurikkuse strateegia",
    # nature restoration (suggested)
    "looduse taastamine",
    "ökosüsteemide taastamine",
  ],

  "fi": [
    "luontoystävällinen suunnittelu",
    "luontopositiivinen",
    # nature-based solutions (verified variants used in EU texts)
    "luontoon perustuvat ratkaisut",
    "luontopohjaiset ratkaisut",
    # biodiversity strategy
    "biodiversiteettistrategia",
    "EU:n biodiversiteettistrategia",
    # nature restoration (suggested; frequently “ennallistaminen” in EU/FI context)
    "luonnon ennallistaminen",
    "ekosysteemien ennallistaminen",
  ],

  "fr": [
    "aménagement respectueux de la nature",
    "positif pour la nature",
    # nature-based solutions (verified)
    "solutions fondées sur la nature",
    "solution fondée sur la nature",
    # net gain (kept; also add generic net gain)
    "gain net",
    "gain net pour la biodiversité",
    # biodiversity strategy
    "stratégie pour la biodiversité",
    "stratégie de l'UE en faveur de la biodiversité",
    # nature restoration (suggested)
    "restauration de la nature",
    "restauration des écosystèmes",
  ],

  "hr": [
    "dizajn usklađen s prirodom",
    "pozitivan za prirodu",
    # nature-based solutions (verified)
    "rješenja temeljena na prirodi",
    "rješenja utemeljena na prirodi",
    # net gain (kept; consider biodiversity-specific)
    "neto dobitak za prirodu",
    "neto dobitak za bioraznolikost",
    # biodiversity strategy
    "strategija bioraznolikosti",
    "strategija EU-a za bioraznolikost",
    # nature restoration (suggested)
    "obnova prirode",
    "obnova ekosustava",
  ],

  "hu": [
    "természetközeli tervezés",
    "természetpozitív",
    # nature-based solutions (verified)
    "természetalapú megoldások",
    # net gain (kept; add biodiversity-specific)
    "nettó természetnyereség",
    "nettó biodiverzitásnyereség",
    # biodiversity strategy
    "biodiverzitás-stratégia",
    "uniós biodiverzitás-stratégia",
    # nature restoration (suggested)
    "a természet helyreállítása",
    "ökoszisztémák helyreállítása",
  ],

  "ga": [
    "dearadh atá comhtháite leis an dúlra",
    "dearfach don dúlra",
    # nature-based solutions (verified)
    "réitigh bunaithe ar an dúlra",
    "réitigh atá bunaithe ar an dúlra",
    # net gain (kept; biodiversity-specific suggested)
    "glanbuntáiste don dúlra",
    "glanbuntáiste don bhithéagsúlacht",
    # biodiversity strategy
    "straitéis bithéagsúlachta",
    "straitéis bithéagsúlachta an AE",
    # nature restoration (suggested)
    "athchóiriú an dúlra",
    "athchóiriú éiceachóras",
  ],

  "it": [
    "progettazione a favore della natura",
    "positivo per la natura",
    # nature-based solutions (verified)
    "soluzioni basate sulla natura",
    "soluzione basata sulla natura",
    # net gain (verified concept in EU strategy)
    "guadagno netto",
    "guadagno netto di biodiversità",
    "guadagno netto per la biodiversità",
    # biodiversity strategy
    "strategia sulla biodiversità",
    "strategia dell'UE sulla biodiversità",
    # nature restoration (verified/suggested; EU texts often use “ripristino”)
    "ripristino della natura",
    "ripristino degli ecosistemi",
  ],

  "lt": [
    "su gamta suderintas dizainas",
    "teigiamas gamtai",
    # nature-based solutions (verified)
    "gamtos procesais pagrįsti sprendimai",
    # net gain (kept; biodiversity-specific suggested)
    "grynasis aplinkosaugos pelnas",
    "grynasis biologinės įvairovės prieaugis",
    # biodiversity strategy
    "biologinės įvairovės strategija",
    "ES biologinės įvairovės strategija",
    # nature restoration (suggested)
    "gamtos atkūrimas",
    "ekosistemų atkūrimas",
  ],

  "lv": [
    "dizains saskaņā ar dabu",
    "pozitīvs dabai",
    # nature-based solutions (verified)
    "dabā balstīti risinājumi",
    # net gain (kept; biodiversity-specific suggested)
    "neto ieguvums dabai",
    "neto ieguvums bioloģiskajai daudzveidībai",
    # biodiversity strategy
    "bioloģiskās daudzveidības stratēģija",
    "ES bioloģiskās daudzveidības stratēģija",
    # nature restoration (suggested)
    "dabas atjaunošana",
    "ekosistēmu atjaunošana",
  ],

  "mt": [
    "disinn li jinkludi n-natura",
    "pożittiv għan-natura",
    # nature-based solutions (verified)
    "soluzzjonijiet ibbażati fuq in-natura",
    # net gain (kept; biodiversity-specific suggested)
    "qligħ nett għall-ambjent",
    "qligħ nett għall-bijodiversità",
    # biodiversity strategy
    "strateġija tal-bijodiversità",
    "strateġija tal-UE għall-bijodiversità",
    # nature restoration (suggested; EU Maltese often uses “restawr” language)
    "restawr tan-natura",
    "restawr tal-ekosistemi",
  ],

  "nl": [
    "natuurinclusief ontwerp",
    "natuurinclusief",
    "natuurpositief",
    # nature-based solutions (verified)
    "op de natuur gebaseerde oplossingen",
    "natuurgebaseerde oplossingen",
    # net gain (kept)
    "netto natuurwinst",
    "netto biodiversiteitswinst",
    # biodiversity strategy
    "biodiversiteitsstrategie",
    "EU-biodiversiteitsstrategie",
    # nature restoration (suggested)
    "natuurherstel",
    "herstel van de natuur",
  ],

  "pl": [
    "projektowanie przyjazne przyrodzie",
    "pozytywny dla przyrody",
    # nature-based solutions (verified)
    "rozwiązania oparte na przyrodzie",
    "rozwiązania oparte na zasobach przyrody",
    # net gain (kept; biodiversity-specific suggested)
    "zysk netto dla środowiska",
    "zysk netto dla bioróżnorodności",
    # biodiversity strategy
    "strategia na rzecz bioróżnorodności",
    "strategia UE na rzecz bioróżnorodności",
    # nature restoration (suggested)
    "odbudowa przyrody",
    "odtwarzanie przyrody",
    "odtwarzanie ekosystemów",
  ],

  "pt": [
    # adjust wording slightly; keep your original but add EUR-Lex anchor terms
    "projeto inclusivo da natureza",
    "positivo para a natureza",
    # nature-based solutions (verified)
    "soluções baseadas na natureza",
    # net gain (kept; add generic net gain)
    "ganho líquido",
    "ganho líquido em biodiversidade",
    "ganho líquido para a biodiversidade",
    # biodiversity strategy
    "estratégia de biodiversidade",
    "estratégia da UE para a biodiversidade",
    # nature restoration (verified in the nature restoration proposal title context)
    "restauração da natureza",
    "restauração dos ecossistemas",
  ],

  "ro": [
    "proiectare în armonie cu natura",
    "pozitiv pentru natură",
    # nature-based solutions (verified)
    "soluții bazate pe natură",
    # net gain (kept)
    "câștig net de biodiversitate",
    "câștig net pentru biodiversitate",
    # biodiversity strategy
    "strategia privind biodiversitatea",
    "strategia UE privind biodiversitatea",
    # nature restoration (verified/suggested)
    "restaurarea naturii",
    "restaurarea ecosistemelor",
  ],

  "sk": [
    "dizajn začleňujúci prírodu",
    "pozitívny pre prírodu",
    # nature-based solutions (verified)
    "riešenia založené na prírode",
    # net gain (kept; biodiversity-specific suggested)
    "čistý zisk pre prírodu",
    "čistý zisk pre biodiverzitu",
    # biodiversity strategy
    "stratégia biodiverzity",
    "stratégia EÚ v oblasti biodiverzity",
    # nature restoration (verified in summary/title usage)
    "obnova prírody",
    "obnova ekosystémov",
  ],

  "sl": [
    "na naravo osredotočeno oblikovanje",
    "pozitivno za naravo",
    # nature-based solutions (verified)
    "na naravi temelječe rešitve",
    "rešitve, ki temeljijo na naravi",
    # net gain (kept; biodiversity-specific suggested)
    "čisti dobiček za naravo",
    "čisti dobiček za biotsko raznovrstnost",
    # biodiversity strategy
    "strategija biotske raznovrstnosti",
    "strategija EU za biotsko raznovrstnost",
    # nature restoration (suggested)
    "obnova narave",
    "obnova ekosistemov",
  ],

  "sv": [
    "naturanpassad design",
    "naturpositiv",
    # nature-based solutions (verified)
    "naturbaserade lösningar",
    "naturbaserad lösning",
    # net gain (kept; biodiversity-specific suggested)
    "nettofördel för naturen",
    "nettofördel för biologisk mångfald",
    # biodiversity strategy
    "biodiversitetsstrategi",
    "EU:s biodiversitetsstrategi",
    # nature restoration (suggested)
    "restaurering av natur",
    "återställande av naturen",
  ],

  "en": [
    "nature-positive", "nature positive",
    "nature inclusive", "nature-inclusive",
    "nature inclusive design", "nature-inclusive design",
    "nature based solutions", "nature-based solutions",
    "biodiversity net gain", "biodiversity net-gain",
    "biodiversity gain",
    "nature restoration",
    "nature repair",
    "biodiversity strategy",
  ],
}

#SEARCH_TERMS_FULLTEXT = ['nature-positive', 'nature positive', 'nature inclusive', 'nature-inclusive', 'nature inclusive design', 'nature-inclusive design', 'nature based solutions', 'nature-based solutions', 'net gain', 'net-gain', 'biodiversity net gain', 'biodiversity net-gain', 'biodiversity gain', 'nature restoration', 'nature repair', 'biodiversity strategy', 'offshore wind', 'offshore wind', 'marine energy', 'blue carbon', 'blue economy', 'restoration', 'nature capital', 'natural capital', 'nature-based', 'nature based', 'green infrastructure', 'marine infrastructure', 'infrastructure', 'mitigation', 'offsetting', 'NiD', 'NBS', 'NbS', 'design']
SEARCH_TERMS_FULLTEXT = ['nature-positive', 'nature positive', 'nature inclusive', 'nature-inclusive', 'nature inclusive design', 'nature-inclusive design', 'nature based solutions', 'nature-based solutions', 'biodiversity net gain', 'net gain', 'net-gain' 'biodiversity net-gain', 'biodiversity gain', 'nature restoration', 'nature repair', 'biodiversity strategy']
TRANSLATED_TERMS_FULLTEXT = {
  "bg": [
    # nature-inclusive / nature-inclusive design (kept)
    "интегриран с природата дизайн",
    "природосъобразен дизайн",
    # nature-positive (kept)
    "природопозитивен",
    # nature-based solutions (verified)
    "природосъобразни решения",
    # biodiversity strategy (kept)
    "стратегия за биологичното разнообразие",
    "стратегия на ЕС за биологичното разнообразие",
    # nature restoration (suggested supplement; EU texts also use “възстановяване” wording)
    "възстановяване на природата",
    "възстановяване на екосистемите",
  ],

  "cs": [
    "návrh zahrnující přírodu",
    "pozitivní pro přírodu",
    # nature-based solutions (verified)
    "řešení založená na přírodě",
    # biodiversity strategy
    "strategie v oblasti biologické rozmanitosti",
    "strategie EU v oblasti biologické rozmanitosti",
    # nature restoration (suggested; EU also uses obnova přírody in nature restoration context)
    "obnova přírody",
    "obnova ekosystémů",
  ],

  "da": [
    "naturinkluderende",
    "naturinkluderende design",
    "naturpositiv",
    # nature-based solutions (verified)
    "naturbaserede løsninger",
    # biodiversity strategy
    "biodiversitetsstrategi",
    "EU's biodiversitetsstrategi",
    # nature restoration (verified/suggested)
    "genopretning af natur",
    "genopretning af biodiversitet",
  ],

  "de": [
    "naturverträgliche Planung",
    "naturpositiv",
    # nature-based solutions (verified)
    "naturbasierte Lösungen",
    "naturbasierte Lösung",
    # net gain (verified in EU biodiversity strategy wording)
    "Netto-Gewinn",
    "Netto-Gewinn für die Biodiversität",
    # biodiversity strategy
    "Biodiversitätsstrategie",
    "EU-Biodiversitätsstrategie",
    # nature restoration (verified/suggested)
    "Wiederherstellung der Natur",
    "Wiederherstellung von Ökosystemen",
  ],

  "el": [
    "φιλικός προς τη φύση σχεδιασμός",
    "θετικός για τη φύση",
    # nature-based solutions (verified)
    "λύσεις βασισμένες στη φύση",
    # net gain (kept, but more biodiversity-specific likely improves precision)
    "καθαρό περιβαλλοντικό κέρδος",
    "καθαρό κέρδος για τη βιοποικιλότητα",
    # biodiversity strategy
    "στρατηγική για τη βιοποικιλότητα",
    "στρατηγική της ΕΕ για τη βιοποικιλότητα",
    # nature restoration (suggested)
    "αποκατάσταση της φύσης",
    "αποκατάσταση οικοσυστημάτων",
  ],

  "es": [
    "diseño respetuoso con la naturaleza",
    "positivo para la naturaleza",
    # nature-based solutions (verified)
    "soluciones basadas en la naturaleza",
    # net gain (aligned to EU strategy language)
    "ganancia neta",
    "ganancia neta en biodiversidad",
    "ganancia neta para la biodiversidad",
    # biodiversity strategy
    "estrategia de biodiversidad",
    "estrategia de la UE sobre la biodiversidad",
    # nature restoration (EU texts use “recuperación”; regulation context often uses “restauración”)
    "recuperación de la naturaleza",
    "restauración de la naturaleza",
    "restauración de los ecosistemas",
  ],

  "et": [
    "loodusega arvestav disain",
    "looduspositiivne",
    # nature-based solutions (verified)
    "looduspõhised lahendused",
    "looduspõhine lahendus",
    # biodiversity strategy
    "elurikkuse strateegia",
    "ELi elurikkuse strateegia",
    # nature restoration (suggested)
    "looduse taastamine",
    "ökosüsteemide taastamine",
  ],

  "fi": [
    "luontoystävällinen suunnittelu",
    "luontopositiivinen",
    # nature-based solutions (verified variants used in EU texts)
    "luontoon perustuvat ratkaisut",
    "luontopohjaiset ratkaisut",
    # biodiversity strategy
    "biodiversiteettistrategia",
    "EU:n biodiversiteettistrategia",
    # nature restoration (suggested; frequently “ennallistaminen” in EU/FI context)
    "luonnon ennallistaminen",
    "ekosysteemien ennallistaminen",
  ],

  "fr": [
    "aménagement respectueux de la nature",
    "positif pour la nature",
    # nature-based solutions (verified)
    "solutions fondées sur la nature",
    "solution fondée sur la nature",
    # net gain (kept; also add generic net gain)
    "gain net",
    "gain net pour la biodiversité",
    # biodiversity strategy
    "stratégie pour la biodiversité",
    "stratégie de l'UE en faveur de la biodiversité",
    # nature restoration (suggested)
    "restauration de la nature",
    "restauration des écosystèmes",
  ],

  "hr": [
    "dizajn usklađen s prirodom",
    "pozitivan za prirodu",
    # nature-based solutions (verified)
    "rješenja temeljena na prirodi",
    "rješenja utemeljena na prirodi",
    # net gain (kept; consider biodiversity-specific)
    "neto dobitak za prirodu",
    "neto dobitak za bioraznolikost",
    # biodiversity strategy
    "strategija bioraznolikosti",
    "strategija EU-a za bioraznolikost",
    # nature restoration (suggested)
    "obnova prirode",
    "obnova ekosustava",
  ],

  "hu": [
    "természetközeli tervezés",
    "természetpozitív",
    # nature-based solutions (verified)
    "természetalapú megoldások",
    # net gain (kept; add biodiversity-specific)
    "nettó természetnyereség",
    "nettó biodiverzitásnyereség",
    # biodiversity strategy
    "biodiverzitás-stratégia",
    "uniós biodiverzitás-stratégia",
    # nature restoration (suggested)
    "a természet helyreállítása",
    "ökoszisztémák helyreállítása",
  ],

  "ga": [
    "dearadh atá comhtháite leis an dúlra",
    "dearfach don dúlra",
    # nature-based solutions (verified)
    "réitigh bunaithe ar an dúlra",
    "réitigh atá bunaithe ar an dúlra",
    # net gain (kept; biodiversity-specific suggested)
    "glanbuntáiste don dúlra",
    "glanbuntáiste don bhithéagsúlacht",
    # biodiversity strategy
    "straitéis bithéagsúlachta",
    "straitéis bithéagsúlachta an AE",
    # nature restoration (suggested)
    "athchóiriú an dúlra",
    "athchóiriú éiceachóras",
  ],

  "it": [
    "progettazione a favore della natura",
    "positivo per la natura",
    # nature-based solutions (verified)
    "soluzioni basate sulla natura",
    "soluzione basata sulla natura",
    # net gain (verified concept in EU strategy)
    "guadagno netto",
    "guadagno netto di biodiversità",
    "guadagno netto per la biodiversità",
    # biodiversity strategy
    "strategia sulla biodiversità",
    "strategia dell'UE sulla biodiversità",
    # nature restoration (verified/suggested; EU texts often use “ripristino”)
    "ripristino della natura",
    "ripristino degli ecosistemi",
  ],

  "lt": [
    "su gamta suderintas dizainas",
    "teigiamas gamtai",
    # nature-based solutions (verified)
    "gamtos procesais pagrįsti sprendimai",
    # net gain (kept; biodiversity-specific suggested)
    "grynasis aplinkosaugos pelnas",
    "grynasis biologinės įvairovės prieaugis",
    # biodiversity strategy
    "biologinės įvairovės strategija",
    "ES biologinės įvairovės strategija",
    # nature restoration (suggested)
    "gamtos atkūrimas",
    "ekosistemų atkūrimas",
  ],

  "lv": [
    "dizains saskaņā ar dabu",
    "pozitīvs dabai",
    # nature-based solutions (verified)
    "dabā balstīti risinājumi",
    # net gain (kept; biodiversity-specific suggested)
    "neto ieguvums dabai",
    "neto ieguvums bioloģiskajai daudzveidībai",
    # biodiversity strategy
    "bioloģiskās daudzveidības stratēģija",
    "ES bioloģiskās daudzveidības stratēģija",
    # nature restoration (suggested)
    "dabas atjaunošana",
    "ekosistēmu atjaunošana",
  ],

  "mt": [
    "disinn li jinkludi n-natura",
    "pożittiv għan-natura",
    # nature-based solutions (verified)
    "soluzzjonijiet ibbażati fuq in-natura",
    # net gain (kept; biodiversity-specific suggested)
    "qligħ nett għall-ambjent",
    "qligħ nett għall-bijodiversità",
    # biodiversity strategy
    "strateġija tal-bijodiversità",
    "strateġija tal-UE għall-bijodiversità",
    # nature restoration (suggested; EU Maltese often uses “restawr” language)
    "restawr tan-natura",
    "restawr tal-ekosistemi",
  ],

  "nl": [
    "natuurinclusief ontwerp",
    "natuurinclusief",
    "natuurpositief",
    # nature-based solutions (verified)
    "op de natuur gebaseerde oplossingen",
    "natuurgebaseerde oplossingen",
    # net gain (kept)
    "netto natuurwinst",
    "netto biodiversiteitswinst",
    # biodiversity strategy
    "biodiversiteitsstrategie",
    "EU-biodiversiteitsstrategie",
    # nature restoration (suggested)
    "natuurherstel",
    "herstel van de natuur",
  ],

  "pl": [
    "projektowanie przyjazne przyrodzie",
    "pozytywny dla przyrody",
    # nature-based solutions (verified)
    "rozwiązania oparte na przyrodzie",
    "rozwiązania oparte na zasobach przyrody",
    # net gain (kept; biodiversity-specific suggested)
    "zysk netto dla środowiska",
    "zysk netto dla bioróżnorodności",
    # biodiversity strategy
    "strategia na rzecz bioróżnorodności",
    "strategia UE na rzecz bioróżnorodności",
    # nature restoration (suggested)
    "odbudowa przyrody",
    "odtwarzanie przyrody",
    "odtwarzanie ekosystemów",
  ],

  "pt": [
    # adjust wording slightly; keep your original but add EUR-Lex anchor terms
    "projeto inclusivo da natureza",
    "positivo para a natureza",
    # nature-based solutions (verified)
    "soluções baseadas na natureza",
    # net gain (kept; add generic net gain)
    "ganho líquido",
    "ganho líquido em biodiversidade",
    "ganho líquido para a biodiversidade",
    # biodiversity strategy
    "estratégia de biodiversidade",
    "estratégia da UE para a biodiversidade",
    # nature restoration (verified in the nature restoration proposal title context)
    "restauração da natureza",
    "restauração dos ecossistemas",
  ],

  "ro": [
    "proiectare în armonie cu natura",
    "pozitiv pentru natură",
    # nature-based solutions (verified)
    "soluții bazate pe natură",
    # net gain (kept)
    "câștig net de biodiversitate",
    "câștig net pentru biodiversitate",
    # biodiversity strategy
    "strategia privind biodiversitatea",
    "strategia UE privind biodiversitatea",
    # nature restoration (verified/suggested)
    "restaurarea naturii",
    "restaurarea ecosistemelor",
  ],

  "sk": [
    "dizajn začleňujúci prírodu",
    "pozitívny pre prírodu",
    # nature-based solutions (verified)
    "riešenia založené na prírode",
    # net gain (kept; biodiversity-specific suggested)
    "čistý zisk pre prírodu",
    "čistý zisk pre biodiverzitu",
    # biodiversity strategy
    "stratégia biodiverzity",
    "stratégia EÚ v oblasti biodiverzity",
    # nature restoration (verified in summary/title usage)
    "obnova prírody",
    "obnova ekosystémov",
  ],

  "sl": [
    "na naravo osredotočeno oblikovanje",
    "pozitivno za naravo",
    # nature-based solutions (verified)
    "na naravi temelječe rešitve",
    "rešitve, ki temeljijo na naravi",
    # net gain (kept; biodiversity-specific suggested)
    "čisti dobiček za naravo",
    "čisti dobiček za biotsko raznovrstnost",
    # biodiversity strategy
    "strategija biotske raznovrstnosti",
    "strategija EU za biotsko raznovrstnost",
    # nature restoration (suggested)
    "obnova narave",
    "obnova ekosistemov",
  ],

  "sv": [
    "naturanpassad design",
    "naturpositiv",
    # nature-based solutions (verified)
    "naturbaserade lösningar",
    "naturbaserad lösning",
    # net gain (kept; biodiversity-specific suggested)
    "nettofördel för naturen",
    "nettofördel för biologisk mångfald",
    # biodiversity strategy
    "biodiversitetsstrategi",
    "EU:s biodiversitetsstrategi",
    # nature restoration (suggested)
    "restaurering av natur",
    "återställande av naturen",
  ],

  "en": [
    "nature-positive", "nature positive",
    "nature inclusive", "nature-inclusive",
    "nature inclusive design", "nature-inclusive design",
    "nature based solutions", "nature-based solutions",
    "biodiversity net gain", "biodiversity net-gain",
    "biodiversity gain",
    "nature restoration",
    "nature repair",
    "biodiversity strategy",
  ],
}


SOURCE_TO_COUNTRY = {'UK': 'UK', 'UK_GOVUK': 'UK', 'US': 'US', 'Canada': 'Canada', 'CA': 'Canada', 'Australia': 'Australia', 'AUS': 'Australia', 'AU': 'Australia', 'New Zealand': 'New Zealand', 'NZ': 'New Zealand'}
PATHS_BY_COUNTRY = {'Australia': 'country_dfs/nid_policy_aus_legislation.csv', 'New Zealand': 'country_dfs/nid_policy_nz_legislation.csv', 'Canada': 'country_dfs/nid_policy_publications_gc_ca.csv', 'UK': 'country_dfs/nid_policy_uk_legislation.csv', 'US': 'country_dfs/nid_policy_regulations_gov.csv'}

# Backward-compatible aliases used by the standalone retrieval modules.
NON_EU_SEARCH_TERMS_PRIMARY = SEARCH_TERMS_PRIMARY
NON_EU_SEARCH_TERMS_SECONDARY = SEARCH_TERMS_FULLTEXT

#POSITIVE_HINTS = ("positiv", "pozitiv", "positivo", "positif", "pozyt", "pozitī", "θετικ", "dearfach", "природопозитив", "naturpositiv", "luontopositiiv", "looduspositiiv")
#DESIGN_HINTS = ("design", "planung", "plán", "plan", "progett", "projekt", "aménagement", "oblikov", "dizajn", "diseño", "ontwerp", "tervez", "suunnittelu", "disinn", "dearadh",    "proiectare", "projektowanie", "načrt", "oblikovanje")

#BNG_BIODIV_HINTS = ("biodivers", "biodiverz", "bioraz", "biolog", "elurikk", "vielfalt", "bioróżnor", "biodiversidad")
#BNG_NET_HINTS    = ("net", "nett", "netto", "gain", "ganancia", "guadagno", "ganho", "câștig", "zysk", "dobit", "κέρδ", "qligħ")

def dedupe_terms(terms: list[str]) -> list[str]:
    seen = set()
    out = []
    for term in terms:
        if term not in seen:
            out.append(term)
            seen.add(term)
    return out


def flatten_translated_terms(term_map: dict[str, list[str]]) -> list[str]:
    out: list[str] = []
    for terms in term_map.values():
        out.extend(terms)
    return dedupe_terms(out)
