# Novels Baltigo

Bot de Telegram para novels em portugues, com busca, obra, capitulos e leitura em Telegraph.

## O que ja esta pronto

- `/start` com destaques e deep links
- `/novel` e `/buscar` para procurar obras
- painel da obra
- lista paginada de capitulos
- leitura por Telegraph
- PDF por capitulo
- EPUB por capitulo
- pre-aquecimento do proximo capitulo em Telegraph
- referrals, broadcast e metricas

## Fluxo

1. Busque com `/novel nome da obra`
2. Abra a obra
3. Escolha um capitulo
4. Leia pelo Telegraph
5. Use anterior/proximo para seguir lendo
6. Baixe o PDF ou EPUB se quiser guardar o capitulo

## Configuracao minima

Copie `.env.example` para `.env` e preencha pelo menos:

- `BOT_TOKEN`
- `BOT_USERNAME`
- `REQUIRED_CHANNEL`
- `REQUIRED_CHANNEL_URL`

Se quiser painel de admin completo, preencha tambem:

- `ADMIN_IDS`
- `CANAL_POSTAGEM_NOVELS`
- `CANAL_POSTAGEM_NOVEL_CAPITULOS`
- `PDF_PROTECT_CONTENT` se quiser restringir encaminhamento do PDF

Exemplo de separacao de canais:

- `CANAL_POSTAGEM_NOVELS=@NovelsBrasil`
- `CANAL_POSTAGEM_NOVEL_CAPITULOS=@AtualizacoesOn`

## Instalacao

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Se estiver em VPS Linux e o Playwright reclamar de dependencias do sistema:

```bash
python -m playwright install --with-deps chromium
```

## Rodar

```bash
python bot.py
```

## Observacoes

- A fonte principal configurada hoje e `https://centralnovel.com`.
- O cliente usa `httpx` primeiro e cai para Playwright quando a fonte exigir sessao de navegador.
- O cache fica dentro de `data/`, incluindo o cache de paginas do Telegraph.
- O Telegraph e o PDF recebem o banner configurado em `PROMO_BANNER_URL`.
