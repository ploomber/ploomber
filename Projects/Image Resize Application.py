from tkinter import *
import tkinter as tk
from tkinter import filedialog, messagebox
import os
import cv2

img = ''


def browseing():
    global img
    fln = filedialog.askopenfilename(initialdir=os.getcwd(), title="Browse Image File", filetypes=(("JPG Image", "*.jpg"), ("PNG Image", "*.png"), ("All Files", "*.*")))
    t1.set(fln)
    img = cv2.imread(fln, cv2.IMREAD_UNCHANGED)
    w.set(img.shape[0])
    h.set(img.shape[1])


def previewing():
    cv2.imshow("Source Image", img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()


def recalculate():
    p = int(perc.get())
    new_width = int(int(w.get()) * p / 100)
    new_height = int(int(h.get()) * p/100)
    w.set(new_width)
    h.set(new_height)


def preview_resized_img():
    nw = int(w.get())
    nh = int(h.get())
    img2 = cv2.resize(img, (nw, nh), interpolation=cv2.INTER_AREA)
    cv2.imshow("Preview Resized Image", img2)
    cv2.waitKey(0)
    cv2.destroyAllWindows()


def save_resized_img():
    fln = filedialog.asksaveasfilename(initialdir=os.getcwd(), title="Save Image", filetypes=(("JPG File", "*.jpg"), ("PNG File", "*.png"), ("All Files", "*.*")))
    nw = int(w.get())
    nh = int(h.get())
    img2 = cv2.resize(img, (nw, nh), interpolation=cv2.INTER_AREA)
    cv2.imwrite(fln, img2)
    messagebox.showinfo("Image Saved", "Image has been saved to"+os.path.basename(fln)+"successfully.")


y = Tk()
t1 = StringVar()
w = StringVar()
h = StringVar()
perc = StringVar()

wrapper = LabelFrame(y, text="Source File")
wrapper.pack(fill="both", expand="yes", padx=20, pady=20)

wrapper2 = LabelFrame(y, text="Image Details")
wrapper2.pack(fill="both", expand="yes", padx=20, pady=20)

# wrapper3 = LabelFrame(y, text="Actions")
# wrapper3.pack(fill="both", expand="yes", padx=20, pady=20)

lbl = Label(wrapper, text="Source File")
lbl.pack(side=tk.LEFT, padx=10, pady=10)

ent = Entry(wrapper, textvariable=t1)
ent.pack(side=tk.LEFT, padx=10, pady=10)

btn = Button(wrapper, text="Browse", command=browseing)
btn.pack(side=tk.LEFT, padx=10, pady=10)

btn2 = Button(wrapper, text="Preview", command=previewing)
btn2.pack(side=tk.LEFT, padx=10, pady=10)

lbl2 = Label(wrapper2, text="Dimension")
lbl2.pack(side=tk.LEFT, padx=10, pady=10)

ent2 = Entry(wrapper2, textvariable=w)
ent2.pack(side=tk.LEFT, padx=10, pady=10)

lbl3 = Label(wrapper2, text="x")
lbl3.pack(side=tk.LEFT, padx=10, pady=10)

ent3 = Entry(wrapper2, textvariable=h)
ent3.pack(side=tk.LEFT, padx=5, pady=10)

wrapper4 = LabelFrame(y, text="Pixel Safe")
wrapper4.pack(fill="both", expand="yes", padx=20, pady=20)

lbl4 = Label(wrapper4, text="Precentage")
lbl4.pack(side=tk.LEFT, padx=10, pady=10)

ent4 = Entry(wrapper4, textvariable=perc)
ent4.pack(side=tk.LEFT, padx=10, pady=10)

btn3 = Button(wrapper4, text="Recalculate Dimension", command=recalculate)
btn3.pack(side=tk.LEFT, padx=10, pady=10)

wrapper5 = LabelFrame(y, text="Action")
wrapper5.pack(fill="both", expand="yes", padx=20, pady=20)

prvbtn = Button(wrapper5, text="Preview", command=preview_resized_img)
prvbtn.pack(side=tk.LEFT, padx=10, pady=10)

savebtn = Button(wrapper5, text="Save", command=save_resized_img)
savebtn.pack(side=tk.LEFT, padx=10, pady=10)

y.title("Image Resizer")
y.geometry("800x600")
y.mainloop()
