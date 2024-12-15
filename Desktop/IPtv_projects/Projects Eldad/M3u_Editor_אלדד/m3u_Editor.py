from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout, QPushButton, QFileDialog,
                             QTextEdit, QInputDialog, QListWidget, QListWidgetItem, QComboBox,
                             QHBoxLayout, QLabel, QMessageBox, QDialog, QLineEdit)
from PyQt5.QtCore import Qt
import sys
import re

class MoveChannelsDialog(QDialog):
    def __init__(self, parent=None, categories=None):
        super().__init__(parent)
        self.categories = categories or []
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Move Channels')
        layout = QVBoxLayout(self)

        self.categoryCombo = QComboBox(self)
        self.categoryCombo.addItems(self.categories)
        layout.addWidget(QLabel("Select category to move to:"))
        layout.addWidget(self.categoryCombo)

        self.newCategoryInput = QLineEdit(self)
        layout.addWidget(QLabel("Or create a new category:"))
        layout.addWidget(self.newCategoryInput)

        buttonBox = QHBoxLayout()
        self.okButton = QPushButton('OK', self)
        self.cancelButton = QPushButton('Cancel', self)
        buttonBox.addWidget(self.okButton)
        buttonBox.addWidget(self.cancelButton)
        layout.addLayout(buttonBox)

        self.okButton.clicked.connect(self.accept)
        self.cancelButton.clicked.connect(self.reject)

    def getSelectedCategory(self):
        if self.newCategoryInput.text():
            return self.newCategoryInput.text()
        return self.categoryCombo.currentText()

class M3UEditor(QWidget):
    def __init__(self):
        super().__init__()
        self.categories = {}
        self.initUI()

    def initUI(self):
        self.setWindowTitle('M3U Playlist Editor')
        self.setGeometry(100, 100, 800, 600)

        main_layout = QVBoxLayout(self)
        title = QLabel("M3U Playlist Editor", self)
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 24px; font-weight: bold;")
        main_layout.addWidget(title)

        main_layout.addLayout(self.create_category_section())
        main_layout.addLayout(self.create_channel_section())
        main_layout.addLayout(self.create_m3u_content_section())

        # New button to filter Israel-related channels
        self.filterButton = QPushButton('Filter Israel Channels', self)
        self.filterButton.clicked.connect(self.filterIsraelChannels)
        main_layout.addWidget(self.filterButton)

        # New button to save filtered channels
        self.saveFilteredButton = QPushButton('Save Filtered Channels', self)
        self.saveFilteredButton.clicked.connect(self.saveFilteredChannels)
        main_layout.addWidget(self.saveFilteredButton)

    def create_category_section(self):
        layout = QVBoxLayout()
        category_title = QLabel("Categories", self)
        category_title.setAlignment(Qt.AlignCenter)
        category_title.setStyleSheet("font-size: 18px; font-weight: bold;")
        layout.addWidget(category_title)

        button_layout = QHBoxLayout()
        self.addCategoryButton = QPushButton('Add Category')
        self.updateCategoryButton = QPushButton('Update Category Name')
        self.deleteCategoryButton = QPushButton('Delete Selected')
        self.moveCategoryUpButton = QPushButton('Move Category Up')
        self.moveCategoryDownButton = QPushButton('Move Category Down')
        self.selectDeselectAllButton = QPushButton('Select All / Deselect All')

        button_layout.addWidget(self.addCategoryButton)
        button_layout.addWidget(self.updateCategoryButton)
        button_layout.addWidget(self.deleteCategoryButton)
        button_layout.addWidget(self.moveCategoryUpButton)
        button_layout.addWidget(self.moveCategoryDownButton)
        button_layout.addWidget(self.selectDeselectAllButton)

        layout.addLayout(button_layout)
        self.categoryList = QListWidget(self)
        layout.addWidget(self.categoryList)

        self.addCategoryButton.clicked.connect(self.addCategory)
        self.updateCategoryButton.clicked.connect(self.updateCategoryName)
        self.deleteCategoryButton.clicked.connect(self.deleteSelectedCategories)
        self.moveCategoryUpButton.clicked.connect(self.moveCategoryUp)
        self.moveCategoryDownButton.clicked.connect(self.moveCategoryDown)
        self.selectDeselectAllButton.clicked.connect(self.toggleSelectDeselectAll)
        self.categoryList.itemClicked.connect(self.display_channels)

        return layout

    def create_channel_section(self):
        layout = QVBoxLayout()
        channel_title = QLabel("Channels", self)
        channel_title.setAlignment(Qt.AlignCenter)
        channel_title.setStyleSheet("font-size: 18px; font-weight: bold;")
        layout.addWidget(channel_title)

        button_layout = QHBoxLayout()
        self.addChannelButton = QPushButton('Add Channel')
        self.deleteChannelButton = QPushButton('Delete Selected')
        self.moveChannelUpButton = QPushButton('Move Up')
        self.moveChannelDownButton = QPushButton('Move Down')
        self.selectAllChannelsButton = QPushButton('Select All')
        self.clearChannelsSelectionButton = QPushButton('Deselect All')
        self.moveSelectedChannelButton = QPushButton('Move Selected')
        self.editSelectedChannelButton = QPushButton('Edit Selected')

        button_layout.addWidget(self.addChannelButton)
        button_layout.addWidget(self.deleteChannelButton)
        button_layout.addWidget(self.moveChannelUpButton)
        button_layout.addWidget(self.moveChannelDownButton)
        button_layout.addWidget(self.selectAllChannelsButton)
        button_layout.addWidget(self.clearChannelsSelectionButton)
        button_layout.addWidget(self.moveSelectedChannelButton)
        button_layout.addWidget(self.editSelectedChannelButton)

        layout.addLayout(button_layout)
        self.channelList = QListWidget(self)
        self.channelList.setSelectionMode(QListWidget.MultiSelection)
        layout.addWidget(self.channelList)

        self.addChannelButton.clicked.connect(self.addChannel)
        self.deleteChannelButton.clicked.connect(self.deleteSelectedChannels)
        self.moveChannelUpButton.clicked.connect(self.moveChannelUp)
        self.moveChannelDownButton.clicked.connect(self.moveChannelDown)
        self.selectAllChannelsButton.clicked.connect(self.selectAllChannels)
        self.clearChannelsSelectionButton.clicked.connect(self.deselectAllChannels)
        self.moveSelectedChannelButton.clicked.connect(self.moveSelectedChannel)
        self.editSelectedChannelButton.clicked.connect(self.editSelectedChannel)

        return layout

    def create_m3u_content_section(self):
        layout = QVBoxLayout()
        m3u_title = QLabel("M3U Content", self)
        m3u_title.setAlignment(Qt.AlignCenter)
        m3u_title.setStyleSheet("font-size: 18px; font-weight: bold;")
        layout.addWidget(m3u_title)

        self.textEdit = QTextEdit(self)
        layout.addWidget(self.textEdit)

        button_layout = QHBoxLayout()
        self.loadButton = QPushButton('Load M3U')
        self.saveButton = QPushButton('Save M3U')

        button_layout.addWidget(self.loadButton)
        button_layout.addWidget(self.saveButton)

        layout.addLayout(button_layout)

        self.loadButton.clicked.connect(self.loadM3U)
        self.saveButton.clicked.connect(self.saveM3U)

        return layout

    def addCategory(self):
        category, ok = QInputDialog.getText(self, 'Add Category', 'Enter category name:')
        if ok and category and category not in self.categories:
            self.categories[category] = []
            self.categoryList.addItem(QListWidgetItem(category))

    def updateCategoryName(self):
        selected_item = self.categoryList.currentItem()
        if not selected_item:
            QMessageBox.warning(self, "Warning", "No category selected for updating.")
            return
        
        old_category_name = selected_item.text()
        new_category_name, ok = QInputDialog.getText(self, 'Update Category', 'Enter new category name:', text=old_category_name)
        if ok and new_category_name and new_category_name != old_category_name:
            self.categories[new_category_name] = self.categories.pop(old_category_name)
            self.categoryList.currentItem().setText(new_category_name)
            self.updateM3UContent(old_category_name, new_category_name)

    def deleteSelectedCategories(self):
        selected_items = self.categoryList.selectedItems()
        for item in selected_items:
            category_name = item.text()
            if category_name in self.categories:
                del self.categories[category_name]
                self.removeCategoryFromM3U(category_name)
            self.categoryList.takeItem(self.categoryList.row(item))

    def removeCategoryFromM3U(self, category_name):
        new_m3u_content = []
        for line in self.textEdit.toPlainText().splitlines():
            if not line.startswith("#EXTINF") or category_name not in line:
                new_m3u_content.append(line)
        self.textEdit.setPlainText("\n".join(new_m3u_content))

    def moveCategoryUp(self):
        current_row = self.categoryList.currentRow()
        if current_row > 0:
            current_item = self.categoryList.takeItem(current_row)
            self.categoryList.insertItem(current_row - 1, current_item)
            self.categoryList.setCurrentRow(current_row - 1)

    def moveCategoryDown(self):
        current_row = self.categoryList.currentRow()
        if current_row < self.categoryList.count() - 1:
            current_item = self.categoryList.takeItem(current_row)
            self.categoryList.insertItem(current_row + 1, current_item)
            self.categoryList.setCurrentRow(current_row + 1)

    def toggleSelectDeselectAll(self):
        for i in range(self.categoryList.count()):
            item = self.categoryList.item(i)
            item.setSelected(not item.isSelected())

    def addChannel(self):
        name, ok1 = QInputDialog.getText(self, 'Add Channel', 'Enter channel name:')
        url, ok2 = QInputDialog.getText(self, 'Add Channel', 'Enter channel URL:')
        if ok1 and ok2:
            channel_item = QListWidgetItem(name)
            self.channelList.addItem(channel_item)
            selected_category = self.categoryList.currentItem()
            if selected_category:
                category_name = selected_category.text()
                self.categories[category_name].append(f"{name} ({url})")
                self.updateM3UContent()

    def deleteSelectedChannels(self):
        selected_items = self.channelList.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "No channels selected for deletion.")
            return
        
        current_category = self.categoryList.currentItem().text()
        for item in selected_items:
            channel_info = item.text()
            self.channelList.takeItem(self.channelList.row(item))
            self.categories[current_category] = [
                c for c in self.categories[current_category] if c.split(" (")[0] != channel_info
            ]

        self.updateM3UContent()

    def updateM3UContent(self, old_category_name=None, new_category_name=None):
        m3u_content = ""
        for category, channels in self.categories.items():
            for channel in channels:
                try:
                    name, url = channel.rsplit(" (", 1)
                    m3u_content += f"#EXTINF:-1 group-title=\"{category}\", {name.strip()}\n{url.strip()[:-1]}\n"
                except ValueError:
                    continue
        self.textEdit.setPlainText(m3u_content)

        if old_category_name and new_category_name:
            self.textEdit.setPlainText(self.textEdit.toPlainText().replace(old_category_name, new_category_name))

    def moveChannelUp(self):
        current_row = self.channelList.currentRow()
        if current_row > 0:
            current_item = self.channelList.takeItem(current_row)
            self.channelList.insertItem(current_row - 1, current_item)
            self.channelList.setCurrentRow(current_row - 1)

    def moveChannelDown(self):
        current_row = self.channelList.currentRow()
        if current_row < self.channelList.count() - 1:
            current_item = self.channelList.takeItem(current_row)
            self.channelList.insertItem(current_row + 1, current_item)
            self.channelList.setCurrentRow(current_row + 1)

    def selectAllChannels(self):
        for i in range(self.channelList.count()):
            self.channelList.item(i).setSelected(True)

    def deselectAllChannels(self):
        for i in range(self.channelList.count()):
            self.channelList.item(i).setSelected(False)

    def moveSelectedChannel(self):
        selected_items = self.channelList.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "No channels selected for moving.")
            return

        dialog = MoveChannelsDialog(self, list(self.categories.keys()))
        if dialog.exec_():
            target_category = dialog.getSelectedCategory()
            if target_category not in self.categories:
                self.categories[target_category] = []
                self.categoryList.addItem(QListWidgetItem(target_category))

            current_category = self.categoryList.currentItem().text()
            moved_count = 0
            for item in selected_items:
                channel_name = item.text()
                url = self.getUrl(channel_name)
                full_entry = self.findFullM3UEntry(channel_name, current_category)
                if full_entry:
                    self.categories[target_category].append(full_entry)
                    self.categories[current_category] = [
                        c for c in self.categories[current_category] if c.split(" (")[0] != channel_name
                    ]
                    moved_count += 1

            self.display_channels(self.categoryList.currentItem())
            self.updateM3UContent()
            QMessageBox.information(self, "Success", f"Moved {moved_count} channels to {target_category}")

    def editSelectedChannel(self):
        selected_items = self.channelList.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "No channels selected for editing.")
            return
        
        for item in selected_items:
            channel_info = item.text()
            new_name, ok = QInputDialog.getText(self, 'Edit Channel', 'Edit channel name:', text=channel_info.strip())
            if ok and new_name:
                url = self.getUrl(channel_info)
                item.setText(new_name.strip())
                current_category = self.categoryList.currentItem().text()
                self.categories[current_category] = [
                    f"{new_name.strip()} ({url})" if c.split(" (")[0] == channel_info else c for c in self.categories[current_category]
                ]
                self.updateM3UContent()

    def display_channels(self, item):
        category = item.text()
        self.channelList.clear()
        for channel in self.categories.get(category, []):
            channel_name = channel.split(" (")[0]
            channel_item = QListWidgetItem(channel_name)
            self.channelList.addItem(channel_item)

    def getUrl(self, channel_info):
        try:
            return channel_info.split(" (")[1][:-1]
        except IndexError:
            return ""

    def findFullM3UEntry(self, channel_name, category):
        for channel in self.categories.get(category, []):
            if channel.startswith(channel_name):
                return channel
        return None

    def loadM3U(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getOpenFileName(self, "Open M3U File", "", "M3U Files (*.m3u *.m3u8);;All Files (*)", options=options)
        if fileName:
            try:
                with open(fileName, 'r', encoding='utf-8') as file:
                    content = file.read()
            except UnicodeDecodeError:
                with open(fileName, 'r', encoding='latin-1') as file:
                    content = file.read()
            self.textEdit.setPlainText(content)
            self.parseM3UContent(content)

    def saveM3U(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getSaveFileName(self, "Save M3U File", "", "M3U Files (*.m3u);;All Files (*)", options=options)
        if fileName:
            with open(fileName, 'w', encoding='utf-8') as file:
                file.write(self.textEdit.toPlainText())

    def parseM3UContent(self, content):
        self.categories.clear()
        category_pattern = re.compile(r'#EXTINF.*group-title="([^"]+)".*,(.*)\n(.*)')

        for match in category_pattern.findall(content):
            group_title, channel_name, channel_url = match
            if group_title not in self.categories:
                self.categories[group_title] = []
            self.categories[group_title].append(f"{channel_name.strip()} ({channel_url.strip()})")

        self.categoryList.clear()
        for category in self.categories.keys():
            self.categoryList.addItem(QListWidgetItem(category))

    def filterIsraelChannels(self):
        israel_keywords = ['ישראל', 'IL', 'ISRAEL', 'עברית', 'hebrew', 'israeli']
        filtered_channels = { 'Movies': [], 'News': [], 'Kids': [], 'Entertainment': [], 'Sports': [] }
        
        for category, channels in self.categories.items():
            for channel in channels:
                if any(keyword in channel for keyword in israel_keywords):
                    filtered_category = self.getFilteredCategory(channel)
                    filtered_channels[filtered_category].append(channel)

        self.categories = filtered_channels
        self.categoryList.clear()
        for category, channels in self.categories.items():
            if channels:  # Only add categories that have channels
                self.categoryList.addItem(QListWidgetItem(category))
        self.updateM3UContent()

    def getFilteredCategory(self, channel):
        if 'חדשות' in channel or 'News' in channel:
            return 'News'
        elif 'סרטים' in channel or 'Movies' in channel:
            return 'Movies'
        elif 'ילדים' in channel or 'Kids' in channel:
            return 'Kids'
        elif 'ספורט' in channel or 'Sports' in channel:
            return 'Sports'
        else:
            return 'Entertainment'

    def saveFilteredChannels(self):
        options = QFileDialog.Options()
        fileName, _ = QFileDialog.getSaveFileName(self, "Save Filtered Channels", "", "M3U Files (*.m3u);;All Files (*)", options=options)
        if fileName:
            with open(fileName, 'w', encoding='utf-8') as file:
                for category, channels in self.categories.items():
                    for channel in channels:
                        try:
                            name, url = channel.rsplit(" (", 1)
                            file.write(f"#EXTINF:-1 group-title=\"{category}\", {name.strip()}\n{url.strip()[:-1]}\n")
                        except ValueError:
                            continue

def main():
    app = QApplication(sys.argv)
    editor = M3UEditor()
    editor.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
