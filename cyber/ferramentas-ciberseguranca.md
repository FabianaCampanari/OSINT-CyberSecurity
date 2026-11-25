# Guia Completo de Ferramentas Essenciais de Cibersegurança & OSINT

Este documento reúne um panorama prático e detalhado sobre 12 ferramentas e plataformas indispensáveis para profissionais de cibersegurança, pesquisadores de inteligência de fontes abertas (OSINT) e hackers éticos, baseado nas fontes do projeto.

---

## Tabela Comparativa de Ferramentas

| # | Title / Website | Primary Use Case | Free or Paid |
| :-: | :--- | :--- | :--- |
| **1** | **[Shodan](https://www.shodan.io)** | Searches for internet-connected devices (servers, webcams, routers, industrial controls) and open ports for infrastructure reconnaissance. | **Free & Paid** (Free tier with limited results; paid plans for deeper access). |
| **2** | **[LeakIX](https://leakix.net)** | OSINT platform scanning for misconfigured, exposed, or compromised services, databases (Elasticsearch, Redis), Git repos, and `.env` files. | **Free & Paid** (Free web interface; paid API access). |
| **3** | **[GreyNoise](https://greynoise.io)** | Analyzes mass internet scanning traffic to distinguish between benign crawlers, automated noise, and targeted malicious probes. | **Free & Paid** (Free community tier; paid tiers for deeper intelligence). |
| **4** | **[Criminal IP](https://www.criminalip.io)** | AI threat intelligence search engine providing 0–100 threat scores for IPs, domains, and exposed assets based on historical attack behavior. | **Free & Paid** (Free tier with daily search limits; paid plans). |
| **5** | **[urlscan.io](https://urlscan.io)** | Sandboxed URL scanner that inspects websites, DOM elements, network requests, scripts, and screenshots safely. | **Free** (Free service; no account required for basic scans). |
| **6** | **[PublicWWW](https://publicwww.com)** | Source code search engine (HTML, JS, CSS) used for tracking shared code snippets, API keys, tracking IDs, and infrastructure attribution. | **Free & Paid** (Free tier with limited results; paid plans). |
| **7** | **[OSINT Framework](https://osintframework.com)** | Interactive directory mapping hundreds of OSINT tools categorized by investigation domain (emails, usernames, IPs, dark web). | **Free** (100% free community resource). |
| **8** | **[Epios](https://epios.com)** | Passive OSINT tool for email and phone reverse lookups across 140+ platforms, extracting Google profiles and Maps review location data. | **Free & Paid** (Free tier for basic results; paid tiers for full module access). |
| **9** | **[GeoSpy AI](https://geospy.ai)** | AI photo geolocation tool that pinpoints image locations by analyzing visual cues (architecture, vegetation, street layouts) without EXIF metadata. | **Free & Paid** (Free consumer interface; enterprise version available as Raven). |
| **10** | **[AbuseIPDB](https://www.abuseipdb.com)** | Community database tracking reported malicious IP activity (SSH brute force, DDoS, web scraping) with threat confidence scores. | **Free & Paid** (Free for lookups and community reporting; paid API plans). |
| **11** | **[Cisco Talos Intelligence](https://talosintelligence.com)** | Enterprise threat portal providing IP, domain, and file hash reputation lookups using Cisco's global threat telemetry. | **Free** (Free public access; no account required for basic lookups). |
| **12** | **[IPinfo](https://ipinfo.io)** | IP intelligence service providing city-level geolocation, ISP/ASN details, and detection of VPNs, Tor exit nodes, and proxies. | **Free & Paid** (Generous free tier; paid professional API subscriptions). |

---

## Detalhamento das Ferramentas (em Português)

### 1. Shodan
* **Link:** [shodan.io](https://www.shodan.io)
* **Descrição:** Mecanismo de busca especializado que mapeia e indexa continuamente dispositivos conectados à internet pública.
* **O que identifica:** Servidores de aplicação, roteadores, câmeras IP, controladores industriais (SCADA), smart TVs e equipamentos hospitalares.
* **Aplicações práticas:** Mapeamento de superfície de ataque corporativa, localização de portas abertas indesejadas e identificação de serviços desatualizados.

### 2. LeakIX (Leak)
* **Link:** [leakix.net](https://leakix.net)
* **Descrição:** Plataforma de OSINT focada em monitorar e indexar vazamentos de dados e falhas de configuração em serviços expostos na web.
* **O que identifica:** Instâncias abertas de Elasticsearch e Redis, repositórios `.git` expostos e arquivos `.env` contendo senhas e chaves de API.
* **Aplicações práticas:** Auditoria de segurança de infraestrutura para correção preventiva de dados sensíveis vazados.

### 3. GreyNoise
* **Link:** [greynoise.io](https://greynoise.io)
* **Descrição:** Plataforma de inteligência de ameaças que monitora e analisa a varredura global da internet.
* **O que identifica:** Distingue entre scanners inofensivos (pesquisadores, robôs de busca) e varreduras maliciosas automatizadas.
* **Aplicações práticas:** Redução de alertas falsos-positivos em centros de operações de segurança (SOC).

### 4. Criminal IP
* **Link:** [criminalip.io](https://www.criminalip.io)
* **Descrição:** Motor de busca e inteligência contra ameaças baseado em IA que classifica riscos digitais em uma escala de 0 a 100.
* **O que identifica:** Nível de perigo de IPs, domínios e ativos com base em histórico de ataques, botnets e hospedagem maliciosa.
* **Aplicações práticas:** Triagem rápida de reputação de domínios e bloqueio preventivo de infraestrutura de ataque.

### 5. urlscan.io
* **Link:** [urlscan.io](https://urlscan.io)
* **Descrição:** Scanner de URLs que analisa páginas da web em um ambiente isolado (*sandbox*).
* **O que identifica:** Capturas de tela do site, requisições de rede, certificados SSL, scripts executados e arquivos baixados.
* **Aplicações práticas:** Investigação segura de links suspeitos e páginas de *phishing* sem risco para a máquina local.

### 6. PublicWWW
* **Link:** [publicwww.com](https://publicwww.com)
* **Descrição:** Mecanismo de busca focado no código-fonte de páginas web (HTML, JavaScript e CSS).
* **O que identifica:** Trechos de códigos compartilhados, IDs de rastreamento (AdSense, Analytics), chaves de API públicas e widgets personalizados.
* **Aplicações práticas:** Atribuição de infraestrutura, permitindo vincular diferentes sites criados pela mesma pessoa ou organização.

### 7. OSINT Framework
* **Link:** [osintframework.com](https://osintframework.com)
* **Descrição:** Diretório interativo em formato de árvore de conhecimento que categoriza centenas de recursos OSINT gratuitos.
* **O que identifica:** Ferramentas organizadas por objetivo de investigação (e-mails, nomes de usuário, redes sociais, IPs, dark web).
* **Aplicações práticas:** Guia de referência para estruturar investigações de inteligência cibernética.

### 8. Epios
* **Link:** [epios.com](https://epios.com)
* **Descrição:** Ferramenta passiva de busca reversa para e-mails e números de telefone.
* **O que identifica:** Presença de e-mails em mais de 140 plataformas online e extração de perfis do Google (incluindo histórico de avaliações no Google Maps).
* **Aplicações práticas:** Investigação de identidade e análise de pegada digital sem alertar o alvo.

### 9. GeoSpy AI
* **Link:** [geospy.ai](https://geospy.ai)
* **Descrição:** Plataforma de inteligência artificial voltada para a geolocalização de imagens.
* **O que identifica:** Localização geográfica exata a partir de análise visual de arquitetura, placas, vegetação e traçado urbano.
* **Aplicações práticas:** Geolocalização de fotos mesmo quando os metadados EXIF foram removidos.

### 10. AbuseIPDB
* **Link:** [abuseipdb.com](https://www.abuseipdb.com)
* **Descrição:** Banco de dados colaborativo mantido pela comunidade global para denúncia e consulta de IPs maliciosos.
* **O que identifica:** Pontuação de confiança de abuso baseada em denúncias de ataques de força bruta, spam e varredura de portas.
* **Aplicações práticas:** Verificação e validação rápida de endereços IP envolvidos em incidentes de segurança.

### 11. Cisco Talos Intelligence
* **Link:** [talosintelligence.com](https://talosintelligence.com)
* **Descrição:** Central de inteligência de ameaças da Cisco alimentada pela telemetria global da empresa.
* **O que identifica:** Reputação de IPs, domínios, assinaturas de ameaças e *hashes* de arquivos maliciosos.
* **Aplicações práticas:** Análise profunda de inteligência de ameaças para defesa de redes corporativas.

### 12. IPinfo
* **Link:** [ipinfo.io](https://ipinfo.io)
* **Descrição:** Serviço de geolocalização e inteligência sobre endereços IP.
* **O que identifica:** Cidade, provedor (ISP), número de sistema autônomo (ASN), além de identificação de proxies, VPNs e nós de saída Tor.
* **Aplicações práticas:** Enriquecimento de logs de segurança e verificação de origem de tráfego de rede.

---

## Metodologia Prática: Atribuição de Domínio e Identificação de Proprietários

Para descobrir quem é o responsável ou proprietário de um site suspeito ou desconhecido, recomenda-se a seguinte sequência investigativa utilizando as ferramentas listadas:

1. **Atribuição por Código-Fonte (PublicWWW):**
   * Inspecione o HTML/JS do site alvo.
   * Procure por IDs únicos (Google Analytics, AdSense, chaves públicas de API).
   * Busque essas assinaturas no PublicWWW para correlacionar outros sites pertencentes ao mesmo proprietário.

2. **Análise de Infraestrutura e Redes (urlscan.io & Criminal IP):**
   * Execute uma varredura isolada no urlscan.io para mapear certificados SSL, IPs de destino e domínios relacionados.
   * Consulte o Criminal IP para obter a pontuação de risco e identificar infraestruturas associadas.

3. **Verificação de Vazamentos e Serviços (Shodan & LeakIX):**
   * Busque o IP do servidor no Shodan para identificar portas abertas e serviços ativos.
   * Cheque no LeakIX se o servidor expõe arquivos de configuração (`.env`, `.git/config`) com dados de desenvolvedores.

4. **Identificação de Rede e Provedor (IPinfo & Cisco Talos):**
   * Determine o ASN e o provedor de hospedagem no IPinfo.
   * Verifique o histórico de reputação do domínio e IP no Cisco Talos Intelligence.

5. **Investigação de Identidade (OSINT Framework & Epios):**
   * Utilize o OSINT Framework para selecionar ferramentas adicionais de histórico de WHOIS e DNS.
   * Caso encontre um e-mail de administrador ou desenvolvedor, consulte no Epios para levantar perfis digitais e localização.
