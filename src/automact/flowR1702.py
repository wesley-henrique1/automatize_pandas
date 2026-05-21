import pyautogui
import time

class QuebraTudo:
    def __init__(self):
        raw_input = input("Cole a lista de códigos aqui:\n")
        
        if raw_input.strip():
            self.automatc(raw_input)
        else:
            print("Lista vazia! Aí não, patrão.")

    def automatc(self, texto_bruto):
        lista_preliminar = texto_bruto.replace(',', ' ').replace(';', ' ').split()
        
        prod_list = []
        for item in lista_preliminar:
            if item.strip().isdigit():
                prod_list.append(item.strip())
        
        if not prod_list:
            print("Nenhum código numérico encontrado.")
            return

        print(f"\n[OK] Encontrados {len(prod_list)} produtos.")
        print("Você tem 5 segundos para clicar no campo do WinThor...")
        for i in range(5, 0, -1):
            print(f"Preparando em {i}...", end="\r", flush=True)
            time.sleep(1)
        print("\n\nVALENDO! Iniciando automação...\n\n")
        total = len(prod_list)
        for i, var in enumerate(prod_list):
            print(f"Digitando: {var} || sequencia {i + 1}/{total}", end="\r", flush=True)

            pyautogui.write(var)
            pyautogui.press('enter')
            time.sleep(0.1)
            
            pyautogui.press('right')

            time.sleep(0.1)
            
            pyautogui.press('enter')
            pyautogui.press('enter')

            time.sleep(0.1)

        print("\nAutomação finalizada! O lucro é nosso!")

if __name__ == "__main__":
    QuebraTudo()
    print("\nPressione Enter para sair...")
    input()